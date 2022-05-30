import { DynamodbEntityConnector } from './connectors/dynamodb-entity-connector';

async function testDynamodbEntityConnector() {
  const client = new DynamodbEntityConnector('pbaa-customers', 'customerId');

  const newCustomer = {
    customerId: 'TEST',
    name: 'Max Mustermann',
  };
  await client.storeItem(newCustomer);

  const readCustomer = await client.getItem({ customerId: 'TEST' });

  console.log(readCustomer);

  
}

async function allTests() {
  // await testDynamodbEntityConnector();
}

allTests().then(() => {
  console.log('finished');
});
