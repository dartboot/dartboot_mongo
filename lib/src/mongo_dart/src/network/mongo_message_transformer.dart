part of mongo_dart;

class MongoMessageHandler {
  final _log = Log('MongoMessageTransformer');
  final converter = PacketConverter();

  void handleData(List<int> data, EventSink<MongoReplyMessage> sink) {
    converter.addPacket(data);
    while (!converter.messages.isEmpty) {
      var buffer = BsonBinary.from(converter.messages.removeFirst());
      var reply = MongoReplyMessage();
      reply.deserialize(buffer);
      _log.debug(() => reply.toString());
      sink.add(reply);
    }
  }

  void handleDone(EventSink<MongoReplyMessage> sink) {
    if (!converter.isClear) {
      _log.warning(
          'Invalid state of PacketConverter in handleDone: $converter');
    }
    sink.close();
  }

  StreamTransformer<List<int>, MongoReplyMessage> get transformer =>
      StreamTransformer<List<int>, MongoReplyMessage>.fromHandlers(
          handleData: handleData, handleDone: handleDone);
}
