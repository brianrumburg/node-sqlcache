import { Template } from 'meteor/templating';
import { Orders, OrderDetails, ProductTypes, RecentOrders } from '../collections';

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

Template.recentOrders.helpers({
  recentOrders() {
    return RecentOrders.find().fetch();
  }
});