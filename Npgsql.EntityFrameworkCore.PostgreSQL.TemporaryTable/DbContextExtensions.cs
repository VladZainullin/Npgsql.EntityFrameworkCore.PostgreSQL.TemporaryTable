using System.Text;
using Microsoft.EntityFrameworkCore;

namespace Npgsql.EntityFrameworkCore.PostgreSQL.TemporaryTable;

public static class DbContextExtensions
{
    public static async Task<IQueryable<T>> ConvertToTemporaryTable<T>(
        this DbContext context,
        IEnumerable<T> entities,
        CancellationToken cancellationToken) where T : class
    {
        await CreateTemporaryTableAsync<T>(context, cancellationToken);
        await InsertEntitiesAsync(context, entities, cancellationToken);

        var tableName = context.Model.FindEntityType(typeof(T))!.GetViewName();

        return context.Set<T>().FromSqlRaw($"select * from {tableName}");
    }

    private static async Task CreateTemporaryTableAsync<T>(
        DbContext context,
        CancellationToken cancellationToken)
    {
        var entityType = context.Model.FindEntityType(typeof(T))!;

        var builder = new StringBuilder("create temporary table if not exists ");

        var tableName = entityType.GetViewName();

        builder.Append(tableName);
        builder.Append('(');

        foreach (var property in entityType.GetProperties())
        {
            var columnName = property.GetColumnName();
            builder.Append(columnName);
            builder.Append(' ');

            var columnType = property.GetColumnType();
            builder.Append(columnType);

            if (property.IsColumnNullable())
            {
                builder.Append(' ');
                builder.Append("null");
            }

            builder.Append(',');
            builder.Append(' ');
        }

        builder.Remove(builder.Length - 2, 2);
        builder.Append(") on commit drop");

        var sql = builder.ToString();

        await context.Database.ExecuteSqlRawAsync(sql, cancellationToken);
    }

    private static async Task InsertEntitiesAsync<T>(
        DbContext context,
        IEnumerable<T> entities,
        CancellationToken cancellationToken)
    {
        var entityType = context.Model.FindEntityType(typeof(T))!;

        var builder = new StringBuilder("copy ");

        var tableName = entityType.GetViewName();
        builder.Append(tableName);

        builder.Append('(');

        var properties = entityType.GetProperties().ToArray();
        foreach (var property in properties)
        {
            var columnName = property.GetColumnName();
            builder.Append(columnName);

            builder.Append(", ");
        }

        builder.Remove(builder.Length - 2, 2);

        builder.Append(") FROM STDIN (FORMAT BINARY)");

        var sql = builder.ToString();

        var connection = (NpgsqlConnection)context.Database.GetDbConnection();
        await using var writer = await connection.BeginBinaryImportAsync(sql, cancellationToken);

        foreach (var entity in entities)
        {
            await writer.StartRowAsync(cancellationToken);

            foreach (var property in properties)
            {
                var value = property.PropertyInfo?.GetValue(entity);
                await writer.WriteAsync(value, cancellationToken);
            }
        }

        await writer.CompleteAsync(cancellationToken);
        await writer.CloseAsync(cancellationToken);
    }
}