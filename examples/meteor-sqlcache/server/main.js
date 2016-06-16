import { Meteor } from 'meteor/meteor';
import mt from 'masstransit';
import caches from './caches';

Meteor.startup(() => {
  console.log('startup');
  
  var bus = mt.create();

  var onMessage = Meteor.bindEnvironment((message, envelope, queueName, reject) => {
    caches[message.table].refresh().catch((e) => {
      reject(e);
    });

    if(message.table === 'Order') {
      caches.RecentOrder.refresh().catch((e) => {
        reject(e);
      });
    }
  });

  bus.ready(() => {
    bus.subscribe({
      queueName: 'meteor-sqlcache',
      messageType: 'SqlNotifier.Messages:TableChanged'
    }, onMessage);
  });

  bus.init({
    host: 'rabbitmq-test',
    queueNames: ['meteor-sqlcache']
  });

});
