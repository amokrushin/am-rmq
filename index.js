'use strict';

var RmqConnection = require( './lib/rmq.connection' );

module.exports = function( logger ) {
    var rmqConnection = new RmqConnection( logger );

    function rmqChannel( callback ) {
        rmqConnection.connection( function( connection ) {
            connection.createChannel( function( err, channel ) {
                if( err ) return callback( err );
                callback( null, channel );
            } );
        } );
    }

    function rmqChannelKeepAlive( callback ) {
        rmqConnection.keepAlive( function( connection ) {
            connection.createChannel( function( err, channel ) {
                if( err ) return callback( err );
                callback( null, channel );
            } );
        } );
    }

    function rmqQueueReqRes( channel, options, handler ) {
        return function( msg ) {
            var rmqreq = JSON.parse( msg.content.toString() );
            try
            {
                if( msg.properties.replyTo )
                {
                    handler( rmqreq, function( rmqres, ack ) {
                        channel.sendToQueue(
                            msg.properties.replyTo,
                            new Buffer( JSON.stringify( rmqres ) )
                        );
                        if( options.ack ) ack ? channel.ack( msg ) : channel.nack( msg );
                        rmqreq = null;
                        rmqres = null;
                    } );
                }
                else
                {
                    handler( rmqreq );
                    rmqreq = null;
                }
            } catch( e )
            {
                logger.error( e );
            }
        }
    }

    return {
        connect: rmqConnection.start,
        onQueue: function( queue, options, handler ) {
            rmqChannelKeepAlive( function( err, channel ) {
                if( err ) return logger.error( err );
                channel.assertQueue( queue );
                channel.prefetch( options.prefetch || 1 );
                channel.consume( queue, rmqQueueReqRes( channel, options, handler ), {
                    noAck: !options.ack
                } );
            } );
        },
        broadcast: function( exchange, options, rmqreq, rmqres ) {
            rmqChannel( function( err, channel ) {
                if( err ) return logger.error( err );
                channel.assertExchange( exchange, 'fanout', {durable: false} );
                if( rmqres )
                {
                    channel.assertQueue( '', {exclusive: true}, function( err, q ) {
                        if( err ) return logger.error( err );
                        var messages = [];

                        channel.consume( q.queue, function( msg ) {
                            messages.push( JSON.parse( msg.content.toString() ) );
                        }, {noAck: true} );

                        channel.publish( exchange, '', new Buffer( JSON.stringify( rmqreq ) ), {replyTo: q.queue} );

                        setTimeout( function() {
                            channel.deleteQueue( q.queue );
                            channel.close();
                            rmqres( messages );
                        }, options.timeout || 5000 );
                    } );
                }
                else
                {
                    channel.publish( exchange, '', new Buffer( JSON.stringify( rmqreq ) ) );
                }
            } );
        },
        onBroadcast: function( exchange, handler ) {
            rmqChannelKeepAlive( function( err, channel ) {
                if( err ) return logger.error( err );
                channel.assertExchange( exchange, 'fanout', {durable: false} );
                channel.assertQueue( '', {exclusive: true}, function( err, q ) {
                    if( err ) return logger.error( err );
                    channel.bindQueue( q.queue, exchange, '' );

                    channel.consume( q.queue, rmqQueueReqRes( channel, {}, handler ), {
                        noAck: true
                    } );
                } );
            } );
        }
    }

};