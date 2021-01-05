import {DynamodbEntityConnector} from "../src";

test('DynamodbEntityConnector.cleanItem', () => {
    const input = {
        id: 'test',
        foo: '',
        bar: undefined
    };
    const expected = {
        id: 'test'
    };

    const output = DynamodbEntityConnector.cleanItem(input);
    expect(output).toEqual(expected);
});
