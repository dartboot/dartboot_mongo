part of mongo_dart;

class _ConnectionManager {
  final _log = Log('ConnectionManager');
  final Db db;
  final _connectionPool = <String, _Connection>{};
  final replyCompleter = <int, Completer<MongoReplyMessage>>{};
  final sendQueue = Queue<MongoMessage>();
  _Connection _masterConnection;

  _ConnectionManager(this.db);

  _Connection get masterConnection => _masterConnection;

  _Connection get masterConnectionVerified {
    if (_masterConnection != null) {
      return _masterConnection;
    } else {
      throw MongoDartError('No master connection');
    }
  }

  Future _connect(_Connection connection) async {
    await connection.connect();
    var isMasterCommand = DbCommand.createIsMasterCommand(db);
    var replyMessage = await connection.query(isMasterCommand);
    _log.debug(() => replyMessage.documents[0].toString());
    var master = replyMessage.documents[0]["ismaster"] == true;
    connection.isMaster = master;
    if (master) {
      _masterConnection = connection;
    }
    connection.serverCapabilities
        .getParamsFromIstMaster(replyMessage.documents[0]);
    if (db._authenticationScheme == null) {
      if (connection.serverCapabilities.maxWireVersion >= 3) {
        db._authenticationScheme = AuthenticationScheme.SCRAM_SHA_1;
      } else {
        db._authenticationScheme = AuthenticationScheme.MONGODB_CR;
      }
    }
    if (connection.serverConfig.userName == null) {
      _log.debug(() => '$db: ${connection.serverConfig.hostUrl} connected');
    } else {
      await db
          .authenticate(connection.serverConfig.userName,
              connection.serverConfig.password,
              connection: connection)
          .then((v) {
        _log.debug(() => '$db: ${connection.serverConfig.hostUrl} connected');
      });
    }
    return true;
  }

  Future open(WriteConcern writeConcern) {
    return Future.forEach(_connectionPool.keys, (hostUrl) {
      var connection = _connectionPool[hostUrl];
      return _connect(connection);
    }).then((_) {
      db.state = State.OPEN;
      return Future.value(true);
    });
  }

  Future close() {
    while (sendQueue.isNotEmpty) {
      masterConnection._sendBuffer();
    }
    sendQueue.clear();

    return Future.forEach(_connectionPool.keys, (hostUrl) {
      var connection = _connectionPool[hostUrl];
      _log.debug(() => '$db: ${connection.serverConfig.hostUrl} closed');
      return connection.close();
    }).then((_) {
      replyCompleter.clear();
    });
  }

  void addConnection(ServerConfig serverConfig) {
    var connection = _Connection(this, serverConfig);
    _connectionPool[serverConfig.hostUrl] = connection;
  }

  dynamic removeConnection(_Connection connection) {
    return _connectionPool.remove(connection.serverConfig.hostUrl);
  }
}
