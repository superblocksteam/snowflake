import { SnowflakeDatasourceConfiguration } from '@superblocksteam/shared';

import {
  DUMMY_ACTION_CONFIGURATION,
  DUMMY_DB_DATASOURCE_CONFIGURATION,
  DUMMY_EXECUTION_CONTEXT,
  DUMMY_EXPECTED_METADATA,
  DUMMY_EXTRA_PLUGIN_EXECUTION_PROPS,
  DUMMY_QUERY_RESULT
} from '@superblocksteam/shared-backend';

jest.mock('@superblocksteam/shared-backend', () => {
  const originalModule = jest.requireActual('@superblocksteam/shared-backend');
  return {
    __esModule: true,
    ...originalModule,
    CreateConnection: jest.fn((target, name, descriptor) => {
      return descriptor;
    }),
    DestroyConnection: jest.fn((target, name, descriptor) => {
      return descriptor;
    })
  };
});

import { Snowflake } from 'snowflake-promise';
jest.mock('snowflake-promise');

import SnowflakePlugin from '.';

const plugin: SnowflakePlugin = new SnowflakePlugin();
plugin.logger = { debug: (): void => undefined };

const DUMMY_SNOWFLAKE_TABLE_RESULT = [
  {
    COLUMN_NAME: 'id',
    DATA_TYPE: 'int4',
    TABLE_NAME: 'orders',
    TABLE_TYPE: 'BASE TABLE'
  },
  {
    COLUMN_NAME: 'user_id',
    DATA_TYPE: 'int8',
    TABLE_NAME: 'orders',
    TABLE_TYPE: 'BASE TABLE'
  }
];

const context = DUMMY_EXECUTION_CONTEXT;
const datasourceConfiguration = DUMMY_DB_DATASOURCE_CONFIGURATION as SnowflakeDatasourceConfiguration;
const actionConfiguration = DUMMY_ACTION_CONFIGURATION;
const props = {
  context,
  datasourceConfiguration,
  actionConfiguration,
  ...DUMMY_EXTRA_PLUGIN_EXECUTION_PROPS
};

afterEach(() => {
  jest.restoreAllMocks();
});

describe('Snowflake Plugin', () => {
  it('test connection', async () => {
    jest.spyOn(Snowflake.prototype, 'connect').mockImplementation(() => {
      return new Promise((_) => setTimeout(_, 100));
    });
    jest.spyOn(Snowflake.prototype, 'execute').mockImplementation(() => {
      return new Promise((_) => setTimeout(_, 100));
    });

    await plugin.test(datasourceConfiguration);

    expect(Snowflake.prototype.connect).toBeCalledTimes(1);
    expect(Snowflake.prototype.execute).toBeCalledTimes(1);
  });

  it('get metadata', async () => {
    jest.spyOn(Snowflake.prototype, 'connect').mockImplementation(() => {
      return new Promise((_) => setTimeout(_, 100));
    });

    jest.spyOn(Snowflake.prototype, 'execute').mockImplementation(() => {
      return new Promise((_) => setTimeout(_, 100)).then(() => DUMMY_SNOWFLAKE_TABLE_RESULT);
    });

    const res = await plugin.metadata(datasourceConfiguration);

    expect(res.dbSchema?.tables[0]).toEqual(DUMMY_EXPECTED_METADATA);
  });

  it('execute query', async () => {
    jest.spyOn(Snowflake.prototype, 'connect').mockImplementation(() => {
      return new Promise((_) => setTimeout(_, 100));
    });

    jest.spyOn(Snowflake.prototype, 'execute').mockImplementation(() => {
      return new Promise((_) => setTimeout(_, 100)).then(() => DUMMY_QUERY_RESULT);
    });

    const auth = datasourceConfiguration.authentication || {};
    const client = new Snowflake({
      account: auth.custom?.account?.value ?? '',
      username: auth.username ?? '',
      password: auth.password ?? '',
      database: auth.custom?.databaseName?.value ?? '',
      schema: auth.custom?.schema?.value ?? '',
      warehouse: auth.custom?.warehouse?.value ?? ''
    });

    const res = await plugin.executePooled(props, client);

    expect(res.output).toEqual(DUMMY_QUERY_RESULT);
    expect(client.execute).toBeCalledTimes(1);
  });
});
