'use strict';

var amqp = require( 'amqplib/callback_api' ),
    url = require( 'url' ),
    backoff = require( 'backoff' ),
    EventEmitter = require( 'events' ).EventEmitter;

module.exports = function( logger ) {
    var rmq = new EventEmitter(),
        rmqUrl,
        activeConnection,
        sigInt = false;

    logger = logger || console;

    function handleConnection( connection ) {
        connection.once( 'error', function( err ) {
            connection = null;
            rmq.emit( 'connection.error', err );
        } );
        connection.once( 'close', function() {
            connection = null;
            rmq.emit( 'connection.closed' );
        } );
    }

    function tryConnection( callback ) {
        amqp.connect( rmqUrl,
            function( err, connection ) {
                if( err && !err.code ) return; //ampqlib calls callback twice sometimes
                if( err ) return callback( err );
                callback( null, connection );
            }
        );
    }

    function waitConnection() {
        if( activeConnection || sigInt ) return;
        var call = backoff.call(
            function( callback ) {
                if( sigInt )
                {
                    call.abort();
                    return callback();
                }
                var retries = call.getNumRetries();
                if( retries ) rmq.emit( 'connection.reconnect', retries );
                tryConnection( callback );
            },
            function( err, connection ) {
                if( err ) logger.error( 'err', err );
                if( connection ) rmq.emit( 'connection.established', connection );
            }
        );
        call.setStrategy( new backoff.ExponentialStrategy() );
        call.start();
    }

    function closeConnection( callback ) {
        if( activeConnection )
        {
            sigInt = true;
            return activeConnection.close( callback );
        }
        callback();
    }

    function onExit() {
        rmq.stop();
    }

    rmq.start = function( settings, retryOnError, callback ) {
        sigInt = false;
        var newRmqUrl = url.format( {
            protocol: 'amqp',
            slashes: true,
            auth: settings.user + ':' + settings.password,
            host: settings.host,
            pathname: settings.vhost,
            query: {heartbeat: 30}
        } );
        if( activeConnection && rmqUrl === newRmqUrl )
        {
            if( callback ) callback();
            return;
        }
        rmqUrl = newRmqUrl;
        closeConnection( function() {
            if( retryOnError )
            {
                waitConnection();
            }
            else
            {
                tryConnection( function( err, connection ) {
                    if( err )
                    {
                        rmq.emit( 'connection.error', err );
                        if( callback ) callback( rmq.humanizeError( err ) );
                    }
                    else
                    {

                        rmq.emit( 'connection.established', connection );
                        if( callback ) callback();
                    }
                } );
            }
        } );
    };

    rmq.stop = function( callback ) {
        process.removeListener( 'SIGINT', onExit );
        sigInt = true;
        closeConnection( callback || function() {} );
    };

    rmq.humanizeError = function( err ) {
        var message = '';
        if( err && err.code === 'ENOTFOUND' ) message = 'Host ' + err.hostname + ' not found';
        if( err && err.code === 'ECONNREFUSED' ) message = err.address + ':' + err.port + ' connection refused';
        if( err && err.code === 'ECONNRESET' )  message = 'Invalid login or password';
        if( message )
        {
            var error = new Error( message );
            error.code = err.code;
            return error;
        }
        else
        {
            return err;
        }
    };

    rmq.keepAlive = function( callback ) {
        if( activeConnection )
        {
            callback( activeConnection );
        }
        rmq.on( 'connection.established', function( connection ) {
            callback( connection );
        } );
    };

    rmq.connection = function( callback ) {
        if( activeConnection )
        {
            return callback( activeConnection );
        }
        rmq.once( 'connection.established', function( connection ) {
            callback( connection );
        } );
    };

    rmq.on( 'connection.established', function( connection ) {
        handleConnection( connection );
        activeConnection = connection;
        process.removeListener( 'SIGINT', onExit );
        process.once( 'SIGINT', onExit );
        logger.info( 'RabbitMQ connection established' );
    } );

    rmq.on( 'connection.closed', function() {
        logger.info( 'RabbitMQ connection closed' );
        activeConnection = null;
        waitConnection();
    } );

    rmq.on( 'connection.error', function( err ) {
        logger.error( 'RabbitMQ connection error:', rmq.humanizeError( err ) );
    } );

    rmq.on( 'connection.reconnect', function( counter ) {
        logger.info( 'RabbitMQ attempting to reconnect', counter );
    } );

    return rmq;
};