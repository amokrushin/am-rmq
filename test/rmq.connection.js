var assert = require( 'assert' ),
    mockery = require( 'mockery' );

var consoleStub = {
    log: function() {},
    info: function() {},
    error: function() {}
}

describe( 'rmq.connection', function() {
    var RmqConnection;
    var settings = {
        eNotFound: {host: 'ENOTFOUND'},
        eConnRefused: {host: 'ECONNREFUSED'},
        eConnReset: {host: 'ECONNRESET'},
        valid: ''
    }

    before( function() {
        mockery.enable( {
            warnOnReplace: false,
            warnOnUnregistered: false,
            useCleanCache: true
        } );

        function amqplibConnectionStub() {
            var EventEmitter = require( 'events' ).EventEmitter,
                connection = new EventEmitter;
            connection.close = function( callback ) {
                connection.emit( 'close' );
                callback();
            }
            return connection;
        }

        function amqplibMock() {
            return {
                connect: function( url, callback ) {
                    var error = new Error( 'error' );
                    if( /ENOTFOUND/.test( url ) ) error.code = 'ENOTFOUND';
                    if( /ECONNREFUSED/.test( url ) ) error.code = 'ECONNREFUSED';
                    if( /ECONNRESET/.test( url ) ) error.code = 'ECONNRESET';
                    if( error.code ) return callback( error );
                    callback( null, new amqplibConnectionStub() );
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
    } )

    it( 'should establish connection', function( done ) {
        var rmqConnection = RmqConnection( consoleStub );
        rmqConnection.on( 'connection.established', function( connection ) {
            done();
        } );
        rmqConnection.start( settings.valid );
    } )

    it( 'should cause an error', function( done ) {
        var rmqConnection = RmqConnection( consoleStub );
        rmqConnection.on( 'connection.error', function( error ) {
            done();
        } );
        rmqConnection.start( settings.eConnRefused );
    } )

    it( 'should close connection', function( done ) {
        var rmqConnection = RmqConnection( consoleStub );
        var counter = 0;
        rmqConnection.on( 'connection.established', function( connection ) {
            counter++;
            rmqConnection.stop();
        } );
        rmqConnection.on( 'connection.closed', function( connection ) {
            setTimeout( function() {
                if( counter === 1 ) done();
            }, 20 );
        } );
        rmqConnection.start( settings.valid );
    } )

    it( 'should restart connection once', function( done ) {
        var rmqConnection = RmqConnection( consoleStub );
        var counter = 0;
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
        rmqConnection.start( settings.valid );
    } )

    it( 'should try to restart connection', function( done ) {
        this.timeout( 1000 );
        var rmqConnection = RmqConnection( consoleStub );
        var reconnectCounter = 0;
        rmqConnection.on( 'connection.established', function( connection ) {

        } );
        rmqConnection.on( 'connection.reconnect', function( counter ) {
            reconnectCounter = counter;
            if( counter > 2 ) rmqConnection.stop();
        } );
        // x2 100ms+200ms=300ms
        // x3 100ms+200ms+400ms=700ms
        // so delay shoud be more than 700ms
        setTimeout( function() {
            if( reconnectCounter === 3 ) done();
        }, 750 );
        rmqConnection.start( settings.eConnRefused, true );
    } )

    it( 'should not try to restart connection', function( done ) {
        this.timeout( 1000 );
        var rmqConnection = RmqConnection( consoleStub );
        var reconnectCounter = 0;
        rmqConnection.on( 'connection.established', function( connection ) {

        } );
        rmqConnection.on( 'connection.reconnect', function( counter ) {
            reconnectCounter = counter;
            if( counter > 2 ) rmqConnection.stop();
        } );
        // x1 100ms
        // so delay shoud be more than 100ms
        setTimeout( function() {
            if( reconnectCounter === 0 ) done();
        }, 150 ); // first retry may be after 100ms
        rmqConnection.start( settings.eConnRefused, false );
    } )

} );