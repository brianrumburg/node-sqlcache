import { Meteor } from 'meteor/meteor';
import mt from 'masstransit';
import caches from './caches';

Meteor.startup(() => {
  var bus = mt.create();

  bus.ready( Meteor.bindEnvironment(function() {
  	bus.subscribe(
      {queueName: 'meteor-sqlcache', messageType: 'SqlNotifier.Messages:TableChanged'},
      Meteor.bindEnvironment(function(message) {
        caches[message.table].refresh();
  	  }
    ));
  }));

  bus.init({
    host: 'rabbitmq-test',
    queueNames: ['meteor-sqlcache']
  });

  _.forEach(caches, function(c) {
    c.refresh();
  });

});
