'use strict';

var amqp = require( 'amqplib/callback_api' ),
    _ = require( 'lodash' ),
    url = require( 'url' ),
    backoff = require( 'backoff' ),
    EventEmitter = require( 'events' ).EventEmitter;

function rmq( logger ) {
    var connectionSettings,
        sigInt = false,
        rmq = new EventEmitter(),
        activeConnection;

    rmq.on( 'connection.established', function( connection ) {
        handleConnection( connection );
        activeConnection = connection;
        logger.log( 'RabbitMQ connection established' );
    } );

    rmq.on( 'connection.closed', function() {
        logger.log( 'RabbitMQ connection closed' );
        activeConnection = null;
        waitConnection();
    } );

    rmq.on( 'connection.error', function( err ) {
        logger.error( 'RabbitMQ connection error:', err.message );
    } );

    rmq.on( 'connection.reconnect', function( counter ) {
        logger.log( 'RabbitMQ attempting to reconnect', counter );
    } );

    logger = logger || console;

    process.once( 'SIGINT', function() {
        //sigInt = true;
        //if( activeConnection ) activeConnection.close();
    } );

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
        var counter = -1,
            call = backoff.call(
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

    _.extend( rmq, {
        start: function( settings, retryOnError ) {
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
                        if( err ) return rmq.emit( 'connection.error', err );
                        rmq.emit( 'connection.established', connection );
                    } );
                }
            } );
        },
        stop: function( callback ) {
            sigInt = true;
            closeConnection( callback || function() {} );
        },
        humanizeError: function( err ) {
            var message = '';
            if( err && err.code === 'ENOTFOUND' ) message = err.host + ' unavailable';
            if( err && err.code === 'ECONNREFUSED' ) message = err.address + ':' + err.port + ' connection refused';
            if( err && err.code === 'ECONNRESET' )  message = 'Invalid login or password';
            return message;
        }
    } );
    return rmq;
}

module.exports = rmq;