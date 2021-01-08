import {DynamodbEntityConnector} from "../src";

let connector: DynamodbEntityConnector;

beforeAll(() => {
    const tableName = 'test-' + Date.now();
    connector = new DynamodbEntityConnector(tableName, 'id');
})

test('DynamodbEntityConnector: Read and Write', async () => {

    const id = 'test-entity-' + Date.now();
    const writeEntity = {
        id,
        foo: 'bar',
        bar: {
            foo: true
        }
    };

    await connector.storeItem(writeEntity);
    const readEntity = await connector.getItem({id});

    expect(readEntity).toEqual(writeEntity);
});

afterAll(() => {
    return connector.deleteTable();
});

