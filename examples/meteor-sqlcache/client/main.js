import { Template } from 'meteor/templating';
import { Orders, OrderDetails, ProductTypes } from '../collections';

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

Template.productTypes.helpers({
  productTypes() {
    return ProductTypes.find().fetch();
  }
});
