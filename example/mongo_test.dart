import 'package:dartboot_mongo/dartboot_mongo.dart';
import 'package:dartboot_annotation/dartboot_annotation.dart';
import 'package:mongo_dart_query/mongo_dart_query.dart';

/// A test use case.
@Bean()
class MongoTest {
  MongoTest() {
    mongoClient.db('db1').then((client) {
      client
          .collection('t_lesson')
          .count(where.gt('createTime', DateTime.now().millisecondsSinceEpoch))
          .then((value) => print(value));
    });
  }
}
