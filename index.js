'use strict';

var RmqConnection = require( './lib/rmq.connection' );

module.exports = function( logger ) {
    var rmqConnection = new RmqConnection( logger );

    function rmqChannel( callback ) {
        rmqConnection.keepAlive( function( connection ) {
            connection.createChannel( function( err, channel ) {
                if( err ) return callback( err );
                callback( null, channel );
            } );
        } );
    }

    function rmqOnQueue( channel, options, handler ) {
        return function( msg ) {
            var rmqreq = JSON.parse( msg.content.toString() );
            if( msg.properties.replyTo )
            {
                handler( rmqreq, function( rmqres ) {
                    channel.sendToQueue(
                        msg.properties.replyTo,
                        new Buffer( JSON.stringify( rmqres ) )
                    );
                    if( options.ack ) rmqres.ack ? channel.ack( msg ) : channel.nack( msg );
                    rmqreq = null;
                    rmqres = null;
                } );
            }
            else
            {
                handler( rmqreq );
            }
        }
    }

    return {
        connect: rmqConnection.start,
        onQueue: function( queue, options, handler ) {
            rmqChannel( function( err, channel ) {
                if( err ) return logger.error( err ); // logger?
                channel.assertQueue( queue );
                channel.prefetch( options.prefetch || 1 );
                channel.consume( queue, rmqOnQueue( channel, options, handler ), {
                    noAck: !options.ack
                } );
            } );
        },
        onBroadcast: function( exchange, handler ) {
            rmqChannel( function( err, channel ) {
                if( err ) return logger.error( err ); // logger?
                channel.assertExchange( exchange, 'fanout', {durable: false} );
                channel.assertQueue( '', {exclusive: true}, function( err, q ) {
                    if( err ) return logger.error( err );
                    channel.bindQueue( q.queue, exchange, '' );

                    channel.consume( q.queue, rmqOnQueue( channel, {}, handler ), {
                        noAck: true
                    } );
                } );
            } );
        }
    }

};