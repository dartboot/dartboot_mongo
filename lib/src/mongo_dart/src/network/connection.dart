part of mongo_dart;

class _ServerCapabilities {
  int maxWireVersion = 0;
  bool aggregationCursor = false;
  bool writeCommands = false;
  bool authCommands = false;
  bool listCollections = false;
  bool listIndexes = false;
  int maxNumberOfDocsInBatch = 1000;

  void getParamsFromIstMaster(Map<String, dynamic> isMaster) {
    if (isMaster.containsKey('maxWireVersion')) {
      maxWireVersion = isMaster['maxWireVersion'] as int;
    }
    if (maxWireVersion >= 1) {
      aggregationCursor = true;
      authCommands = true;
    }
    if (maxWireVersion >= 2) {
      writeCommands = true;
    }
    if (maxWireVersion >= 3) {
      listCollections = true;
      listIndexes = true;
    }
  }
}

class _Connection {
  final Log _log = Log('Connection');
  final _ConnectionManager _manager;
  ServerConfig serverConfig;
  Socket socket;
  final Set<int> _pendingQueries = {};

  Map<int, Completer<MongoReplyMessage>> get _replyCompleter =>
      _manager.replyCompleter;

  Queue<MongoMessage> get _sendQueue => _manager.sendQueue;
  StreamSubscription<MongoReplyMessage> _repliesSubscription;

  StreamSubscription<MongoReplyMessage> get repliesSubscription =>
      _repliesSubscription;

  bool connected = false;
  bool _closed = false;
  bool isMaster = false;
  final _ServerCapabilities serverCapabilities = _ServerCapabilities();

  _Connection(this._manager, [this.serverConfig]) {
    serverConfig ??= ServerConfig();
  }

  Future<bool> connect() {
    var completer = Completer<bool>();
    Socket.connect(serverConfig.host, serverConfig.port).then((Socket _socket) {
      // Socket connected.
      socket = _socket;
      _repliesSubscription =
          MongoMessageHandler().transformer.bind(socket).listen(_receiveReply,
              onError: (e, st) {
                _log.error('Socket error ${e} ${st}');
                //completer.completeError(e);
                if (!_closed) {
                  _onSocketError();
                }
              },
              cancelOnError: true,
              onDone: () {
                if (!_closed) {
                  _onSocketError();
                }
              });
      connected = true;
      completer.complete(true);
    }).catchError((err) {
      completer.completeError(err);
    });
    return completer.future;
  }

  Future close() {
    _closed = true;
    return socket.close();
  }

  _sendBuffer() {
    _log.debug(() => '_sendBuffer ${!_sendQueue.isEmpty}');
    List<int> message = [];
    while (!_sendQueue.isEmpty) {
      var mongoMessage = _sendQueue.removeFirst();
      message.addAll(mongoMessage.serialize().byteList);
    }
    socket.add(message);
  }

  Future<MongoReplyMessage> query(MongoMessage queryMessage) {
    var completer = Completer<MongoReplyMessage>();
    if (!_closed) {
      _replyCompleter[queryMessage.requestId] = completer;
      _pendingQueries.add(queryMessage.requestId);
      _log.debug(() => 'Query $queryMessage');
      _sendQueue.addLast(queryMessage);
      _sendBuffer();
    } else {
      completer.completeError(const ConnectionException(
          'Invalid state: Connection already closed.'));
    }
    return completer.future;
  }

  ///   If runImmediately is set to false, the message is joined into one packet with
  ///   other messages that follows. This is used for joining insert, update and remove commands with
  ///   getLastError query (according to MongoDB docs, for some reason, these should
  ///   be sent 'together')

  void execute(MongoMessage mongoMessage, bool runImmediately) {
    if (_closed) {
      throw const ConnectionException(
          'Invalid state: Connection already closed.');
    }
    _log.debug(() => 'Execute $mongoMessage');
    _sendQueue.addLast(mongoMessage);
    if (runImmediately) {
      _sendBuffer();
    }
  }

  void _receiveReply(MongoReplyMessage reply) {
    _log.debug(() => reply.toString());
    Completer completer = _replyCompleter.remove(reply.responseTo);
    _pendingQueries.remove(reply.responseTo);
    if (completer != null) {
      _log.debug(() => 'Completing $reply');
      completer.complete(reply);
    } else {
      if (!_closed) {
        _log.error(() => 'Unexpected respondTo: ${reply.responseTo} $reply');
      }
    }
  }

  void _onSocketError() {
    _closed = true;
    var ex = const ConnectionException('connection closed.');
    _pendingQueries.forEach((id) {
      Completer completer = _replyCompleter.remove(id);
      completer.completeError(ex);
    });
    _pendingQueries.clear();
  }
}

class ConnectionException implements Exception {
  final String message;

  const ConnectionException([this.message = '']);

  @override
  String toString() => 'MongoDB ConnectionException: $message';
}
