import { Template } from 'meteor/templating';
import { Orders, OrderDetails } from '../collections';

import './main.html';

Template.orders.helpers({
  orders() {
    return Orders.find().fetch();
  }
});

Template.orderDetails.helpers({
  orderDetails() {
    return OrderDetails.find().fetch();
  }
});
