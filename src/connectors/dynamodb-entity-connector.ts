import * as AWS from 'aws-sdk';
import {AWSError, DynamoDB} from 'aws-sdk';
import {DocumentClient} from 'aws-sdk/lib/dynamodb/document_client';
import {ArrayHelper, WaitHelper} from '@web-academy/core-lib';
import AttributeMap = DocumentClient.AttributeMap;
import BatchGetResponseMap = DocumentClient.BatchGetResponseMap;
import QueryInput = DocumentClient.QueryInput;
import KeyConditions = DocumentClient.KeyConditions;
import FilterConditionMap = DocumentClient.FilterConditionMap;
import UpdateItemInput = DocumentClient.UpdateItemInput;
import ExpressionAttributeValueMap = DocumentClient.ExpressionAttributeValueMap;

export class DynamodbEntityConnector {
  private static BATCH_SIZE = 25;
  private client: DocumentClient;

  protected readonly tableName: string;
  protected readonly partitionKey: string;
  protected readonly sortKey?: string;
  private readonly managementClient: DynamoDB;
  public debug: boolean = false;
  private config: any;

  constructor(tableName: string, partitionKey: string, sortKey?: string, config?: any) {
    this.tableName = tableName;

    if (process.env.DDB_PREFIX) {
      this.tableName = process.env.DDB_PREFIX + this.tableName;
    }

    this.partitionKey = partitionKey;
    this.sortKey = sortKey;

    this.config = config;
    this.client = this.buildClient();
    this.managementClient = this.buildManagementClient();
  }

  async batchGet(keys: any[]): Promise<AttributeMap[] | undefined> {
    const requestedItems = {};
    requestedItems[this.tableName] = {
      Keys: keys,
    };

    const params = {
      RequestItems: requestedItems,
    };
    const response = await this._batchGet(params);

    if (response) return response[this.tableName];
    else return;
  }

  private async _batchGet(
    params: any,
    retry?: boolean
  ): Promise<BatchGetResponseMap | undefined> {
    const self = this;
    return new Promise((resolve, reject) => {
      self.client.batchGet(params, function (err, data) {
        if (err) {
          if (retry) reject(err);
          else self._solveError(err, params, '_batchGet', resolve, reject);
        } else resolve(data.Responses);
      });
    });
  }

  async getItem(key: any, params?: any): Promise<AttributeMap | undefined> {
    return this._getItem({
      TableName: this.tableName,
      Key: key,
      ...params,
    });
  }

  private async _getItem(
    params: any,
    retry?: boolean
  ): Promise<AttributeMap | undefined> {
    const self = this;
    if (this.debug) {
      console.log(
        'DynamodbEntityConnector._getItem',
        params,
        JSON.stringify(params)
      );
    }
    return new Promise((resolve, reject) => {
      self.client.get(params, function (err, data) {
        if (err) {
          if (retry) reject(err);
          else self._solveError(err, params, '_getItem', resolve, reject);
        } else resolve(data.Item);
      });
    });
  }

  async deleteItems(items: any[], params?: any) {
    try {
      const chunks = ArrayHelper.chunk(
        items,
        DynamodbEntityConnector.BATCH_SIZE
      );

      const promises = chunks.map((items) => {
        const tableRequest = items.map((item) => {
          return {
            DeleteRequest: {
              Key: item,
              ...params,
            },
          };
        });

        return this._deleteItems({
          RequestItems: {
            [this.tableName]: tableRequest,
          },
        });
      });

      await Promise.all(promises);
      console.log(
        'Deleted ' + items.length + ' in ' + promises.length + ' Promises'
      );
    } catch (e: any) {
      const message =
        'EXCEPTION in DynamodbEntityConnector.deleteItems for table ' +
        this.tableName;
      console.log(message, e, e.stack);
      throw Error(message);
    }
  }

  private async _deleteItems(params: any, retry?: boolean) {
    const self = this;
    if (this.debug) {
      console.log(
        'DynamodbEntityConnector._deleteItems',
        params,
        JSON.stringify(params)
      );
    }

    return new Promise((resolve, reject) => {
      self.client.batchWrite(params, function (err) {
        if (err) {
          if (retry) reject(err);
          else self._solveError(err, params, '_deleteItems', resolve, reject);
        } else resolve(true);
      });
    });
  }

  async deleteItem(key: any, params?: any) {
    return this._deleteItem({
      TableName: this.tableName,
      Key: key,
      ...params,
    });
  }

  private async _deleteItem(params: any, retry?: boolean) {
    const self = this;
    if (this.debug) {
      console.log(
        'DynamodbEntityConnector._deleteItem',
        params,
        JSON.stringify(params)
      );
    }
    return new Promise((resolve, reject) => {
      self.client.delete(params, function (err, data) {
        if (err) {
          if (retry) reject(err);
          else self._solveError(err, params, '_deleteItem', resolve, reject);
        } else resolve(data.Attributes);
      });
    });
  }

  async storeItems(items: any[]) {
    try {
      const chunks = ArrayHelper.chunk(
        items,
        DynamodbEntityConnector.BATCH_SIZE
      );

      const promises = chunks.map((items) => {
        const tableRequest = items.map((item) => {
          return {
            PutRequest: {
              Item: this._cleanItem(item),
            },
          };
        });

        return this._storeItems({
          RequestItems: {
            [this.tableName]: tableRequest,
          },
        });
      });

      await Promise.all(promises);
      if (this.debug)
      console.log(
        'Stored ' + items.length + ' in ' + promises.length + ' Promises'
      );
    } catch (e: any) {
      console.log(
        'EXCEPTION in DynamodbEntityConnector.storeItems for table ' +
          this.tableName,
        e,
        e.stack
      );
      throw Error(
        'EXCEPTION in DynamodbEntityConnector.storeItems for table ' +
          this.tableName
      );
    }
  }

  private async _storeItems(params: any, retry?: boolean) {
    const self = this;
    if (this.debug) {
      console.log(
        'DynamodbEntityConnector._storeItems',
        params,
        JSON.stringify(params)
      );
    }

    return new Promise((resolve, reject) => {
      self.client.batchWrite(params, function (err) {
        if (err) {
          if (retry) reject(err);
          else self._solveError(err, params, '_storeItems', resolve, reject);
        } else resolve(true);
      });
    });
  }

  async updateItem(
    key: any,
    updateExpression: string,
    expressionAttributeValues?: ExpressionAttributeValueMap
  ): Promise<AttributeMap | undefined> {
    return this._updateItem({
      TableName: this.tableName,
      Key: key,
      UpdateExpression: updateExpression,
      ReturnValues: 'ALL_NEW',
      ExpressionAttributeValues: expressionAttributeValues,
    });
  }

  protected async _updateItem(
    params: UpdateItemInput,
    retry?: boolean
  ): Promise<AttributeMap | undefined> {
    const self = this;
    if (this.debug) {
      console.log(
        'DynamodbEntityConnector._updateItem',
        params,
        JSON.stringify(params)
      );
    }
    return new Promise((resolve, reject) => {
      self.client.update(params, function (err, data) {
        if (err) {
          if (retry) reject(err);
          else self._solveError(err, params, '_updateItem', resolve, reject);
        } else resolve(data.Attributes);
      });
    });
  }

  async storeItem(item: any, params?: any) {
    item = this._cleanItem(item);

    return this._storeItem({
      TableName: this.tableName,
      Item: item,
      ...params,
    });
  }

  private async _storeItem(params: any, retry?: boolean) {
    const self = this;
    if (this.debug) {
      console.log(
        'DynamodbEntityConnector._storeItem',
        params,
        JSON.stringify(params)
      );
    }
    return new Promise((resolve, reject) => {
      self.client.put(params, function (err) {
        if (err) {
          if (retry) reject(err);
          else self._solveError(err, params, '_storeItem', resolve, reject);
        } else resolve(true);
      });
    });
  }

  async scan(
    indexName?: string,
    queryFilter?: any,
    limit?: number,
    additionalParams?: any
  ): Promise<AttributeMap[]> {
    const items: AttributeMap[] = [];

    let params = {
      TableName: this.tableName,
      IndexName: indexName,
      ScanFilter: queryFilter,
      Limit: limit,
      ExclusiveStartKey: null,
    };

    if (additionalParams) {
      params = Object.assign(params, additionalParams);
    }

    let lastEvaluatedKey = null;

    do {
      params.ExclusiveStartKey = lastEvaluatedKey;
      const response = await this._scan(params);

      lastEvaluatedKey = response.LastEvaluatedKey;
      const currentItems: AttributeMap[] = response.Items;

      currentItems.forEach((item) => {
        items.push(item);
      });
    } while (
      lastEvaluatedKey != null &&
      (limit == undefined || items.length < limit)
    );

    return items;
  }

  private async _scan(params: any, retry?: boolean): Promise<any> {
    const self = this;
    return new Promise((resolve, reject) => {
      self.client.scan(params, function (err, data) {
        if (err) {
          if (retry) reject(err);
          else self._solveError(err, params, '_scan', resolve, reject);
        } else resolve(data);
      });
    });
  }

  async query(
    keyConditions: KeyConditions,
    indexName?: string,
    queryFilter?: FilterConditionMap,
    limit?: number,
    additionalParams?: any
  ): Promise<AttributeMap[]> {
    const items: AttributeMap[] = [];
    let params: QueryInput = {
      TableName: this.tableName,
      KeyConditions: keyConditions,
      IndexName: indexName,
      QueryFilter: queryFilter,
      Limit: limit,
      ExclusiveStartKey: undefined,
    };

    if (additionalParams) {
      params = Object.assign(params, additionalParams);
    }

    let lastEvaluatedKey = null;

    do {
      if (lastEvaluatedKey) params.ExclusiveStartKey = lastEvaluatedKey;
      const response = await this._query(params);

      lastEvaluatedKey = response.LastEvaluatedKey;
      const currentItems: AttributeMap[] = response.Items;

      currentItems.forEach((item) => {
        items.push(item);
      });
    } while (
      lastEvaluatedKey != null &&
      (limit == undefined || items.length < limit)
    );

    return items;
  }

  private async _query(params: QueryInput, retry?: boolean): Promise<any> {
    const self = this;
    return new Promise((resolve, reject) => {
      self.client.query(params, function (err, data) {
        if (err) {
          if (retry) reject(err);
          else self._solveError(err, params, '_query', resolve, reject);
        } else resolve(data);
      });
    });
  }

  private _cleanItem(item: any) {
    if (!item) return null;

    let cleanedItem: any = null;
    if (Array.isArray(item)) cleanedItem = [];
    else cleanedItem = {};

    Object.keys(item).forEach((key) => {
      const val = item[key];
      if (typeof val == 'string') {
        if (val != '') cleanedItem[key] = val;
      } else if (typeof val == 'object') {
        cleanedItem[key] = this._cleanItem(val);
      } else {
        cleanedItem[key] = val;
      }
    });

    return cleanedItem;
  }

  async createTable() {
    const attributeDefitions: any[] = [
      {
        AttributeName: this.partitionKey,
        AttributeType: 'S',
      },
    ];

    const keySchema: any[] = [
      {
        AttributeName: this.partitionKey,
        KeyType: 'HASH',
      },
    ];

    if (this.sortKey) {
      attributeDefitions.push({
        AttributeName: this.sortKey,
        AttributeType: 'S',
      });
      keySchema.push({
        AttributeName: this.sortKey,
        KeyType: 'RANGE',
      });
    }

    return await this._createTable({
      TableName: this.tableName,
      BillingMode: 'PAY_PER_REQUEST',
      AttributeDefinitions: attributeDefitions,
      KeySchema: keySchema,
    });
  }

  private async _createTable(params: DynamoDB.Types.CreateTableInput) {
    const client = this.managementClient;
    return new Promise((resolve, reject) => {
      client.createTable(params, function (err, data) {
        if (err) reject(err);
        else resolve(data);
      });
    });
  }

  async deleteTable() {
    return await this._deleteTable({
      TableName: this.tableName,
    });
  }

  private async _deleteTable(params: DynamoDB.Types.DeleteTableInput) {
    const client = this.managementClient;
    return new Promise((resolve, reject) => {
      client.deleteTable(params, function (err, data) {
        if (err) reject(err);
        else resolve(data);
      });
    });
  }

  async describeTable(): Promise<DynamoDB.Types.TableDescription | undefined> {
    return await this._describeTable({
      TableName: this.tableName,
    });
  }

  private async _describeTable(
    params: DynamoDB.Types.DescribeTableInput
  ): Promise<DynamoDB.Types.TableDescription | undefined> {
    const client = this.managementClient;
    return new Promise((resolve, reject) => {
      client.describeTable(params, function (err, data) {
        if (err) reject(err);
        else resolve(data.Table);
      });
    });
  }

  private async _solveError(
    err: AWSError,
    params: any,
    method: string,
    resolve,
    reject
  ) {
    try {
      const code = err.code;
      if (code === 'ResourceNotFoundException') {
        await this.createTable();
        console.log('Created table ' + this.tableName);
        await WaitHelper.wait(2500);

        for (let i = 0; i <= 15; i++) {
          const table = await this.describeTable();
          if (!table) {
            reject(err);
            return;
          }

          if (
            table.TableStatus === 'CREATING' ||
            table.TableStatus === 'UPDATING'
          ) {
            await WaitHelper.wait(2000);
          } else if (table.TableStatus === 'ACTIVE') {
            console.log('Table is active after ' + i + ' cycles');
            break;
          } else {
            reject(err);
            return;
          }

          if (i === 15) {
            console.warn('Timeout in waiting table to be created');
            reject(err);
          }
        }

        // retry
        const result = await this[method](params, true);
        resolve(result);
      } else {
        reject(err);
      }
    } catch (e) {
      reject(err);
    }
  }

  private buildClient(): DocumentClient {
    return new AWS.DynamoDB.DocumentClient(this.buildConfig());
  }

  private buildManagementClient(): AWS.DynamoDB {
    return new AWS.DynamoDB(this.buildConfig());
  }

  private buildConfig() {
    if (this.config) return this.config;

    this.config = {};
    if (process.env.AWS_ACCESS_KEY_ID) {
      this.config.accessKeyId = process.env.AWS_ACCESS_KEY_ID;
      this.config.secretAccessKey = process.env.AWS_SECRET_ACCESS_KEY;
    }

    if (process.env.AWS_REGION) this.config.region = process.env.AWS_REGION;

    if (process.env.AWS_SESSION_TOKEN)
      this.config.sessionToken = process.env.AWS_SESSION_TOKEN;

    return this.config;
  }
}
