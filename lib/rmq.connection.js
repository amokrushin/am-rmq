'use strict';

var amqp = require( 'amqplib/callback_api' ),
    url = require( 'url' ),
    backoff = require( 'backoff' ),
    EventEmitter = require( 'events' ).EventEmitter;

module.exports = function( logger ) {
    var connectionSettings,
        sigInt = false,
        rmq = new EventEmitter(),
        activeConnection;

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
        amqp.connect(
            url.format( {
                protocol: 'amqp',
                slashes: true,
                auth: connectionSettings.user + ':' + connectionSettings.password,
                host: connectionSettings.host,
                pathname: connectionSettings.vhost,
                query: {heartbeat: 30}
            } ),
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
        if( activeConnection ) return activeConnection.close( callback );
        callback();
    }

    function onExit() {
        rmq.stop();
    }

    rmq.start = function( settings, retryOnError, callback ) {
        sigInt = false;
        connectionSettings = settings;
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
        if( err && err.code === 'ENOTFOUND' ) message = 'Host ' + err.host + ' not found';
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

    process.once( 'SIGINT', onExit );

    return rmq;
};