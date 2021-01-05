import * as AWS from "aws-sdk";
import {DocumentClient} from "aws-sdk/lib/dynamodb/document_client";
import AttributeMap = DocumentClient.AttributeMap;
import BatchGetResponseMap = DocumentClient.BatchGetResponseMap;
import {ArrayHelper} from "@web-academy/core-lib";

export class DynamodbEntityConnector {
    private static BATCH_SIZE = 25;

    static async batchGet(tableName: string, keys: any[]): Promise<AttributeMap[] | undefined> {
        const requestedItems = {};
        requestedItems[tableName] = {
            Keys: keys
        };

        const params = {
            RequestItems: requestedItems
        };
        const response = await DynamodbEntityConnector._batchGet(params);

        if (response)
            return response[tableName];
        else
            return;
    }

    static async _batchGet(params): Promise<BatchGetResponseMap | undefined> {
        return new Promise((resolve, reject) => {
            DynamodbEntityConnector.getClient().batchGet(params, function (err, data) {
                if (err) reject(err);
                else resolve(data.Responses);
            });
        });
    }

    static async getItem(tableName: string, key): Promise<AttributeMap | undefined> {
        return DynamodbEntityConnector._getItem({
            TableName: tableName,
            Key: key
        });
    };

    static async _getItem(params): Promise<AttributeMap | undefined> {
        return new Promise((resolve, reject) => {
            DynamodbEntityConnector.getClient().get(params, function (err, data) {
                if (err) reject(err);
                else resolve(data.Item);
            });
        });
    }

    static async deleteItem(tableName: string, key) {
        return DynamodbEntityConnector._deleteItem({
            TableName: tableName,
            Key: key
        });
    };

    static async _deleteItem(params) {
        return new Promise((resolve, reject) => {
            DynamodbEntityConnector.getClient().delete(params, function (err, data) {
                if (err) reject(err);
                else resolve(data.Attributes);
            });
        });
    }

    static async storeItems(tableName: string, items: any[]) {
        try {
            const chunks = ArrayHelper.chunk(items, DynamodbEntityConnector.BATCH_SIZE);

            const promises = chunks.map(items => {
                const tableRequest = items.map(item => {
                    return {
                        PutRequest: {
                            Item: DynamodbEntityConnector.cleanItem(item)
                        }
                    };
                });

                return DynamodbEntityConnector._storeItems({
                    RequestItems: {
                        [tableName]: tableRequest
                    }
                });

            });

            await Promise.all(promises);
            console.log("Stored " + items.length + " in " + promises.length + " Promises");
        } catch (e) {
            console.log("EXCEPTION in DynamodbEntityConnector.storeItems for table " + tableName, e, e.stack);
            throw Error("EXCEPTION in DynamodbEntityConnector.storeItems for table " + tableName);
        }
    };

    static async _storeItems(params) {
        return new Promise((resolve, reject) => {
            DynamodbEntityConnector.getClient().batchWrite(params, function (err) {
                if (err) reject(err);
                else resolve(true);
            });
        });
    }

    static async updateItem(tableName: string, item) {
        item = DynamodbEntityConnector.cleanItem(item);

        return DynamodbEntityConnector._updateItem({
            TableName: tableName,
            Item: item
        });
    };

    static cleanItem(item: any) {
        if (!item)
            return null;

        let cleanedItem: any = null;
        if (Array.isArray(item))
            cleanedItem = [];
        else
            cleanedItem = {};

        Object.keys(item).forEach(key => {
            const val = item[key];
            if (typeof val == 'string') {
                if (val != '')
                    cleanedItem[key] = val;
            } else if (typeof val == 'object') {
                cleanedItem[key] = DynamodbEntityConnector.cleanItem(val);
            } else {
                cleanedItem[key] = val;
            }
        });

        return cleanedItem;
    }

    static async _updateItem(params) {
        return new Promise((resolve, reject) => {
            DynamodbEntityConnector.getClient().put(params, function (err) {
                if (err) reject(err);
                else resolve(true);
            });
        });
    }

    static async scan(tableName: string, indexName?: string, queryFilter?: any, limit?: number, additionalParams?: any): Promise<AttributeMap[]> {

        const items: AttributeMap[] = [];

        let params = {
            TableName: tableName,
            IndexName: indexName,
            ScanFilter: queryFilter,
            Limit: limit,
            ExclusiveStartKey: null
        };

        if (additionalParams) {
            params = Object.assign(params, additionalParams);
        }

        let lastEvaluatedKey = null;

        do {
            params.ExclusiveStartKey = lastEvaluatedKey;
            const response = await DynamodbEntityConnector._scan(params);

            lastEvaluatedKey = response.LastEvaluatedKey;
            const currentItems: AttributeMap[] = response.Items;

            currentItems.forEach(item => {
                items.push(item);
            });

        } while (lastEvaluatedKey != null && (limit == undefined || items.length < limit));

        return items;

    }

    static async _scan(params): Promise<any> {
        return new Promise((resolve, reject) => {
            DynamodbEntityConnector.getClient().scan(params, function (err, data) {
                if (err) reject(err);
                else resolve(data);
            });
        });
    }

    static async query(tableName: string, keyConditions: any, indexName?: string, queryFilter?: any, limit?: number, additionalParams?: any): Promise<AttributeMap[]> {

        const items: AttributeMap[] = [];
        let params = {
            TableName: tableName,
            KeyConditions: keyConditions,
            IndexName: indexName,
            QueryFilter: queryFilter,
            Limit: limit,
            ExclusiveStartKey: null
        };

        if (additionalParams) {
            params = Object.assign(params, additionalParams);
        }

        let lastEvaluatedKey = null;

        do {
            params.ExclusiveStartKey = lastEvaluatedKey;
            const response = await DynamodbEntityConnector._query(params);

            lastEvaluatedKey = response.LastEvaluatedKey;
            const currentItems: AttributeMap[] = response.Items;

            currentItems.forEach(item => {
                items.push(item);
            });

            // console.log("Last Key: " + lastEvaluatedKey);
            // console.log("Items: " + items.length);
            // console.log("Limit: " + limit);

        } while (lastEvaluatedKey != null && (limit == undefined || items.length < limit));

        return items;
    }

    private static async _query(params): Promise<any> {
        return new Promise((resolve, reject) => {
            DynamodbEntityConnector.getClient().query(params, function (err, data) {
                if (err) reject(err);
                else resolve(data);
            });
        });
    }

    static getClient(): DocumentClient {
        return new AWS.DynamoDB.DocumentClient();
    }

}
