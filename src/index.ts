import {
  Column,
  DatasourceMetadataDto,
  ExecutionOutput,
  IntegrationError,
  RawRequest,
  ResolvedActionConfigurationProperty,
  SnowflakeActionConfiguration,
  SnowflakeDatasourceConfiguration,
  Table,
  TableType
} from '@superblocksteam/shared';
import {
  ActionConfigurationResolutionContext,
  BasePlugin,
  normalizeTableColumnNames,
  PluginExecutionProps,
  resolveActionConfigurationPropertyUtil
} from '@superblocksteam/shared-backend';
import { isEmpty } from 'lodash';
import { Snowflake } from 'snowflake-promise';

export default class SnowflakePlugin extends BasePlugin {
  async resolveActionConfigurationProperty({
    context,
    actionConfiguration,
    files,
    property,
    escapeStrings
  }: // eslint-disable-next-line @typescript-eslint/no-explicit-any
  ActionConfigurationResolutionContext): Promise<ResolvedActionConfigurationProperty> {
    return resolveActionConfigurationPropertyUtil(
      super.resolveActionConfigurationProperty,
      {
        context,
        actionConfiguration,
        files,
        property,
        escapeStrings
      },
      false
    );
  }

  async execute({
    context,
    datasourceConfiguration,
    actionConfiguration
  }: PluginExecutionProps<SnowflakeDatasourceConfiguration>): Promise<ExecutionOutput> {
    const client = await this.createClient(datasourceConfiguration);
    try {
      const ret = new ExecutionOutput();
      const query = actionConfiguration.body ?? '';
      if (isEmpty(query)) {
        return ret;
      }
      const warehouse = datasourceConfiguration.authentication?.custom?.warehouse?.value;

      if (warehouse) {
        await client.execute(`USE WAREHOUSE ${warehouse}`);
      }

      const rows = await client.execute(query, context.preparedStatementContext);
      ret.output = normalizeTableColumnNames(rows);
      return ret;
    } catch (err) {
      throw new IntegrationError(`Snowflake query failed, ${err.message}`);
    } finally {
      if (client) await this.destroyClient(client);
    }
  }

  getRequest(actionConfiguration: SnowflakeActionConfiguration): RawRequest {
    return actionConfiguration.body;
  }

  dynamicProperties(): string[] {
    return ['body'];
  }

  async metadata(datasourceConfiguration: SnowflakeDatasourceConfiguration): Promise<DatasourceMetadataDto> {
    const client = await this.createClient(datasourceConfiguration);
    const auth = datasourceConfiguration.authentication;
    const database = auth?.custom?.databaseName?.value ?? '';
    const schema = auth?.custom?.schema?.value;

    let rows;
    // Try both quoted and unquoted calls since we don't know how the identifier was set during creation
    // Ref: https://docs.snowflake.com/en/sql-reference/identifiers-syntax.html
    try {
      try {
        rows = await client.execute(this.getMetadataQuery(database, schema));
      } catch (err) {
        rows = await client.execute(this.getMetadataQuery(database, schema, false));
      }
    } catch (err) {
      throw new IntegrationError(`Fetching Snowflake metadata failed, ${err.message}`);
    }

    const entities = rows.reduce((acc, attribute) => {
      const entityName = attribute['TABLE_NAME'];
      const entityType = attribute['TABLE_TYPE'] === 'BASE TABLE' ? TableType.TABLE : TableType.VIEW;

      const entity = acc.find((o: { name: string }) => o.name === entityName);
      if (entity) {
        const columns = entity.columns;
        entity.columns = [...columns, new Column(attribute.COLUMN_NAME, attribute.DATA_TYPE)];
        return [...acc];
      }

      const table = new Table(entityName, entityType);
      table.columns.push(new Column(attribute.COLUMN_NAME, attribute.DATA_TYPE));

      return [...acc, table];
    }, []);

    this.destroyClient(client);

    return {
      dbSchema: { tables: entities }
    };
  }

  private async createClient(datasourceConfiguration: SnowflakeDatasourceConfiguration): Promise<Snowflake> {
    try {
      const auth = datasourceConfiguration.authentication;
      if (!auth) {
        throw new IntegrationError('Auth not specified for Snowflake action');
      }
      if (!auth.username) {
        throw new IntegrationError('Username not specified for Snowflake action');
      }
      if (!auth.custom?.account?.value) {
        throw new IntegrationError('Account not specified for Snowflake action');
      }

      const client = new Snowflake(
        {
          account: auth.custom?.account?.value,
          username: auth.username,
          password: auth.password,
          database: auth.custom?.databaseName?.value,
          schema: auth.custom?.schema?.value ?? '',
          warehouse: auth.custom?.warehouse?.value ?? ''
        },
        {
          logLevel: 'debug'
        }
      );
      await client.connect();
      const warehouse = auth.custom?.warehouse?.value;
      if (warehouse) {
        await client.execute(`USE WAREHOUSE ${warehouse}`);
      }
      return client;
    } catch (err) {
      throw new IntegrationError(`Snowflake configuration error, ${err.message}`);
    }
  }

  private async destroyClient(client: Snowflake): Promise<void> {
    try {
      await client.destroy();
    } catch (err) {
      throw new IntegrationError(`Client teardown failed, ${err.message}`);
    }
  }

  private getMetadataQuery(database: string, schema?: string, dbNameQuoted = true) {
    let query: string;
    if (dbNameQuoted) {
      query = `select c.TABLE_CATALOG, c.TABLE_SCHEMA, c.TABLE_NAME, c.COLUMN_NAME, c.ORDINAL_POSITION, c.DATA_TYPE, t.TABLE_TYPE
      FROM "${database}"."INFORMATION_SCHEMA"."COLUMNS" as c
      LEFT JOIN "${database}"."INFORMATION_SCHEMA"."TABLES" AS t ON t.TABLE_NAME = c.TABLE_NAME `;
    } else {
      query = `select c.TABLE_CATALOG, c.TABLE_SCHEMA, c.TABLE_NAME, c.COLUMN_NAME, c.ORDINAL_POSITION, c.DATA_TYPE, t.TABLE_TYPE
      FROM ${database}."INFORMATION_SCHEMA"."COLUMNS" as c
      LEFT JOIN ${database}."INFORMATION_SCHEMA"."TABLES" AS t ON t.TABLE_NAME = c.TABLE_NAME `;
    }
    if (schema) {
      query += ` WHERE c.TABLE_SCHEMA ILIKE '${schema}'`;
    }
    query += ` ORDER BY c.TABLE_NAME, c.ORDINAL_POSITION ASC`;

    return query;
  }

  private getTestQuery(database: string, schema?: string, dbNameQuoted = true) {
    if (dbNameQuoted) {
      return `USE "${database}"${schema ? `."${schema}"` : ''}`;
    }
    return `USE ${database}${schema ? `.${schema}` : ''}`;
  }

  async test(datasourceConfiguration: SnowflakeDatasourceConfiguration): Promise<void> {
    let client: Snowflake | undefined;
    if (!datasourceConfiguration) {
      throw new IntegrationError('Datasource not specified for Snowflake plugin');
    }
    const auth = datasourceConfiguration.authentication;
    if (!auth) {
      throw new IntegrationError('Auth not specified for Snowflake plugin');
    }
    const database = auth.custom?.databaseName?.value ?? '';
    const schema = auth.custom?.schema?.value;

    // Try both quoted and unquoted calls since we don't know how the identifier was set during creation
    // Ref: https://docs.snowflake.com/en/sql-reference/identifiers-syntax.html
    try {
      client = await this.createClient(datasourceConfiguration);
      await client.execute(this.getTestQuery(database, schema));
    } catch (err) {
      if (client) {
        try {
          await client.execute(this.getTestQuery(database, schema, false));
        } catch (err) {
          throw new IntegrationError(`Test Snowflake connection failed, ${err.message}`);
        }
      } else {
        throw new IntegrationError(`Test Snowflake connection failed, ${err.message}`);
      }
    } finally {
      if (client) {
        await this.destroyClient(client);
      }
    }
  }
}
