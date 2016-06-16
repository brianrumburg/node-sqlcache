import { Meteor } from 'meteor/meteor';
import mt from 'masstransit';
import caches from './caches';
import Promise from 'promise';

Meteor.startup(() => {
  console.log('startup');
  
  var bus = mt.create();

  var boundMessageHandler = Meteor.bindEnvironment(function(message, fulfill, reject) {

    if(message.table === 'Order') {

      Promise.all([
        caches.Order.refresh(),
        caches.RecentOrder.refresh()
      ])
      .then(() => {
        fulfill();
      })
      .catch((e) => {
        reject(e);
      });

    } else {

      caches[message.table].refresh()
      .then(() => {
        fulfill();
      })
      .catch((e) => {
        reject(e);
      });

    }
  });

  bus.ready(() => {
    bus.subscribe({
      queueName: 'meteor-sqlcache',
      messageType: 'SqlNotifier.Messages:TableChanged'
    }, (message) => {
      return new Promise(function(fulfill, reject) {
        boundMessageHandler(message, fulfill, reject);
      });
    });
  });

  bus.init({
    host: 'rabbitmq-test',
    queueNames: ['meteor-sqlcache']
  });

});
