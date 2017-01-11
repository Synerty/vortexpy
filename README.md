# VortexPY

Synerty's observable, routable, data serialisation and transport code.

Why PY at the end? To be consistent with VortexJS, which is the Browser side package.
See https://github.com/Synerty/vortexjs

Requirements:
* Python 3.5
* Twisted 16+
* SQLAlchemy 1, 1.1

The "vortex" is designed to transport "Payloads" from a web browser (VortexJS) to a
twisted web server (VortexPY). There is also a python client for python client to python server
communication.

### Vortex, The Observable Part

Vortex was designed to allow the Python server to send updates to the browser with out
the browser having to create HTTP request polls.

This is achieved by the browser/client sending one or more payloads to the server as a
HTTP POST, the server then uses this connection to send payloads back to the client.

This HTTP POST connection remains active, Until the client needs
to send more Payloads to the server. To do this, the client simply starts a new HTTP POST
request to the server, the server will then close the old request and begin sending 
Payloads down the respose path of the new request.

Python classes = Vortex

### Transport

Transport occurs over a HTTP(S) layer.

Experimental classes are present to transport data to/from subprocess pipes, this
worked, except the subprocess didn't die and the idea was abandoned
(It was a Celery worker).

Because of this effort, it's much easier to add other transport mechanisms.

Python classes = VortexResource (Receive), VortexConnection (Send)

### Payloads
A "Payload" is a class that stores some routing information, a result and a list of
"Tuples".

The routing information is a dict / json object which looks like this
{key:"anything.type.of.data.list"}
(See Routable for more info)

The tuples attribute stores the data, typically this is a list of Tuple (see Tuples)
objects, however, it can be any data structure that JSON can serialise,
plus Date()/datetime.

The Payload class is responsible for serialising and compressing it's data down to a 
"Vortex Msg", ready for transport. Compression is done with zlib, this imrpovement sees
data reduced to 10% of the original size.

There is custom code that handles converting the structure to JSON objects for json.dumps
so support can be (carefully) added for other data types such as bytes.

Python classes = Payload

### Tuples

Not to be confused with the built in Python tuple syntax (1, 2, 3), Tuples are registered
classes with registered attributes.

Vortex Tuples are important in the design as classes that inherit the vortex Tuple
class will reconstructed as the proper classes at the other end in python. They will have
onlt the fields registerd as TupleFields populated when they are reconstructed.

This is especially useful when using the SQLAlchemy ORM, as the Tuple code recognises
column definitions as TupleFields. 

You can just assign session.query(MyTable).all() to payload.tuples, and the vortex will 
know how to serialise.

From here you can send it do the browser, where the browser may edit some attributes of
the tuples, then send it back to the python server to update the database. 
(More on this in Handlers)

Python classes = Tuple, TupleField

### Routable

When a vortex receives a payload, it is sent to the PayloadIO

The PayloadIO, then passes it on to each PayloadEndpoint that has registered with it.

Payload routing ends with PayloadEndpoints, These endpoints are constructed with another
filter dict/json like object, and a callback.

When PayloadIO tells a PaylaodEndpoint that
a new Payload has arrived, the PayloadEndpoint checks the Payload.filt against it's own 
filt.

If the PayloadEndpoints filt is a subset of the Payload.filt, the PayloadEndpoint then
calls it's callable and passes it the Payload.

PayloadEndpoints register them selves with PayloadIO on construction.
PayloadIO keeps a list of weak refs to the PayloadEndpoints.




Payloads contain a "filt" attribute, which is a json like dict/object.


Python classes = PayloadEndpoint, PayloadIO

### Handlers

Handlers are the provided classes that handle data from the PayloadEndpoints.

* ModelHandler : Simply specify the payload filter and implement the "buildModel" method.
* OrmCrudHandler : This handler used to Create, Update, Retrieve and Delete SQLAlchemy data.
                  It can handle an array of tuples, of different types, and has various
                  hooks allowing the data to be customised before being stored or retreived.
* AsyncModelHandler : Experimental handler that will likely be deleted.


#Change Log

#### 0.3.0

Implemented PY side WebSockets
