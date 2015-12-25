'use strict';

var assert = require( 'assert' ),
    mockery = require( 'mockery' ),
    consoleStub = {
        log: function() {},
        info: function() {},
        error: function() {}
    };

describe( 'rmq.connection', function() {
    var RmqConnection,
        rmqSettings = {
            eNotFound: {host: 'ENOTFOUND'},
            eConnRefused: {host: 'ECONNREFUSED'},
            eConnReset: {host: 'ECONNRESET'},
            valid: ''
        };

    before( function() {
        mockery.enable( {
            warnOnReplace: false,
            warnOnUnregistered: false,
            useCleanCache: true
        } );

        function amqplibConnectionStub() {
            var EventEmitter = require( 'events' ).EventEmitter,
                connection = new EventEmitter();
            connection.close = function( callback ) {
                connection.emit( 'close' );
                callback();
            };
            return connection;
        }

        function amqplibMock() {
            return {
                connect: function( url, callback ) {
                    var err = new Error( 'err' );
                    if( /ENOTFOUND/.test( url ) )
                    {
                        err.code = 'ENOTFOUND';
                        err.host = 'http://example.com'
                    }
                    if( /ECONNREFUSED/.test( url ) )
                    {
                        err.code = 'ECONNREFUSED';
                        err.address = '127.0.0.1';
                        err.port = '5672';
                    }
                    if( /ECONNRESET/.test( url ) ) err.code = 'ECONNRESET';
                    if( err.code ) return callback( err );
                    callback( null, amqplibConnectionStub() );
                }
            };
        }

        mockery.registerMock( 'amqplib/callback_api', amqplibMock() );

        RmqConnection = require( '../lib/rmq.connection' );
    } );

    after( function() {
        mockery.disable();
    } );

    it( 'should inherit from event emitter', function( done ) {
        var rmqConnection = RmqConnection( consoleStub );

        rmqConnection.on( 'foo', done );
        rmqConnection.emit( 'foo' );
    } );

    it( 'should establish connection', function( done ) {
        var rmqConnection = RmqConnection( consoleStub );

        rmqConnection.on( 'connection.established', function( connection ) {
            done();
        } );
        rmqConnection.start( rmqSettings.valid );
    } );

    it( 'should cause an error', function( done ) {
        var rmqConnection = RmqConnection( consoleStub );

        rmqConnection.on( 'connection.error', function( error ) {
            if( error.code === 'ECONNREFUSED' ) done();
        } );
        rmqConnection.start( rmqSettings.eConnRefused );
    } );

    it( 'should close connection', function( done ) {
        var rmqConnection = RmqConnection( consoleStub ),
            counter = 0;

        rmqConnection.on( 'connection.established', function( connection ) {
            counter++;
            rmqConnection.stop();
        } );
        rmqConnection.on( 'connection.closed', function( connection ) {
            setTimeout( function() {
                if( counter === 1 ) done();
            }, 20 );
        } );
        rmqConnection.start( rmqSettings.valid );
    } );

    it( 'should restart connection once', function( done ) {
        var rmqConnection = RmqConnection( consoleStub ),
            counter = 0;

        rmqConnection.on( 'connection.established', function( connection ) {
            counter++;
            if( counter === 2 )
            {
                rmqConnection.stop( function() {
                    setTimeout( function() {
                        if( counter === 2 ) done();
                    }, 20 );
                } );
            }
            if( counter === 1 )
            {
                connection.emit( 'close', new Error( 'connection error' ) );
            }
        } );
        rmqConnection.start( rmqSettings.valid );
    } );

    it( 'should try to restart connection', function( done ) {
        var rmqConnection = RmqConnection( consoleStub ),
            reconnectCounter = 0;

        this.timeout( 1000 );

        rmqConnection.on( 'connection.reconnect', function( counter ) {
            reconnectCounter = counter;
            if( counter > 2 ) rmqConnection.stop();
        } );
        // x2 100ms+200ms=300ms
        // x3 100ms+200ms+400ms=700ms
        // so delay should be more than 700ms
        setTimeout( function() {
            if( reconnectCounter === 3 ) done();
        }, 750 );
        rmqConnection.start( rmqSettings.eConnRefused, true );
    } );

    it( 'should not try to restart connection', function( done ) {
        var rmqConnection = RmqConnection( consoleStub ),
            reconnectCounter = 0;

        this.timeout( 1000 );

        rmqConnection.on( 'connection.established', function( connection ) {

        } );
        rmqConnection.on( 'connection.reconnect', function( counter ) {
            reconnectCounter = counter;
            if( counter > 2 ) rmqConnection.stop();
        } );
        // x1 100ms
        // so delay should be more than 100ms
        setTimeout( function() {
            if( reconnectCounter === 0 ) done();
        }, 150 ); // first retry may be after 100ms
        rmqConnection.start( rmqSettings.eConnRefused, false );
    } );

    it( 'should keep alive connection', function( done ) {
        var rmqConnection = RmqConnection( consoleStub ),
            counter = 0;

        rmqConnection.start( rmqSettings.valid, true );

        rmqConnection.keepAlive( function( connection ) {
            setTimeout( function() {
                if( ++counter === 10 ) return rmqConnection.stop();
                connection.emit( 'close', new Error( 'connection error' ) );
            }, 1 );
        } );

        setTimeout( function() {
            if( counter === 10 ) done();
        }, 20 );
    } );

    it( 'should restart connection with new settings', function( done ) {
        var rmqConnection = RmqConnection( consoleStub ),
            counterEstablished = 0,
            counterClosed = 0;

        rmqConnection.on( 'connection.established', function( connection ) {
            counterEstablished++;
            if( counterEstablished === 1 ) rmqConnection.start( rmqSettings.valid );
            if( counterEstablished === 2 ) rmqConnection.start( rmqSettings.eNotFound );
        } );
        rmqConnection.on( 'connection.closed', function( connection ) {
            counterClosed++;
        } );
        rmqConnection.start( rmqSettings.valid );
        setTimeout( function() {
            if( counterEstablished === 2 && counterClosed === 2 ) done();
        }, 20 );
    } );

    it( 'should test connection (ok)', function( done ) {
        var rmqConnection = RmqConnection( consoleStub );

        rmqConnection.start( rmqSettings.valid, false, function( err ) {
            if( !err ) done();
        } );

    } );

    it( 'should test connection (ENOTFOUND)', function( done ) {
        var rmqConnection = RmqConnection( consoleStub );

        rmqConnection.start( rmqSettings.eNotFound, false, function( err ) {
            if( err.code === 'ENOTFOUND' ) done();
        } );
    } );

    it( 'should test connection (ECONNRESET)', function( done ) {
        var rmqConnection = RmqConnection( consoleStub );

        rmqConnection.start( rmqSettings.eConnReset, false, function( err ) {
            if( err.code === 'ECONNRESET' ) done();
        } );
    } );

    it( 'should test connection (ECONNREFUSED)', function( done ) {
        var rmqConnection = RmqConnection( consoleStub );

        rmqConnection.start( rmqSettings.eConnRefused, false, function( err ) {
            if( err.code === 'ECONNREFUSED' ) done();
        } );
    } );

} );