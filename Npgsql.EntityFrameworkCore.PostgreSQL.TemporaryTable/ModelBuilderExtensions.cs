using System.Reflection;
using Microsoft.EntityFrameworkCore;

namespace Npgsql.EntityFrameworkCore.PostgreSQL.TemporaryTable;

public static class ModelBuilderExtensions
{
    public static ModelBuilder AddTemporaryTables(this ModelBuilder builder, params Assembly[] assemblies)
    {
        foreach (var assembly in assemblies)
        {
            var types = assembly.GetTypes();

            foreach (var type in types)
                if (typeof(ITemporaryTableType).IsAssignableFrom(type))
                    builder.Entity(type).HasNoKey().ToView(default);
        }

        return builder;
    }
}