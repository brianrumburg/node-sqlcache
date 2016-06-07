import { Template } from 'meteor/templating';
import { ReactiveVar } from 'meteor/reactive-var';

import './main.html';

Template.orders.helpers({
  orders() {
    return Orders.find().fetch();
  },
});

Template.orderDetails.helpers({
  orderDetails() {
    return OrderDetails.find().fetch();
  },
});
