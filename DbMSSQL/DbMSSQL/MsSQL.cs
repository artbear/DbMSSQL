using System;
using System.IO;
using System.Data.SqlClient;
using ScriptEngine.Environment;
using ScriptEngine.Machine;
using ScriptEngine.Machine.Contexts;
using ScriptEngine.HostedScript.Library.ValueTable;
using ScriptEngine.HostedScript.Library;
using System.Data;

namespace ScriptEngine.DB.MSSQL
{
    [ContextClass("DbMSSQL", "DbMSSQL")]
    public class MSSQL : AutoContext<MSSQL>, IDisposable
    {

        [ContextProperty("СтрокаПодключения", "ConnectionString")]
        public string ConnectionString { get; set; }

        [ContextProperty("Текст", "Text")]
        public string Text { get; set; }

        [ContextProperty("Параметры", "Parameters")]
        public StructureImpl Parameters { get; set; }


        public MSSQL()
        {
            Parameters = new StructureImpl();
        }


        [ScriptConstructor(Name = "По умолчанию")]
        public static IRuntimeContextInstance Constructor()
        {
            var osdb = new MSSQL();
            return osdb;
        }

        [ContextMethod("УстановитьПараметр", "SetParameter")]
        public void SetParameters(string key, IValue value)
        {
            Parameters.Insert(key, value);
        }

        [ContextMethod("Выполнить", "Execute")]
        public IValue Execute()
        {

            SqlConnection conn = new SqlConnection(this.ConnectionString);

            conn.Open();
            SqlCommand DbCommand = conn.CreateCommand();
            DbCommand.CommandText = this.Text;

            if (Parameters.Count() > 0)
            {

                foreach (KeyAndValueImpl val in Parameters)
                {
                    string keyName = "@" + val.Key.AsString();
                    if (val.Value.DataType == DataType.Boolean)
                    {
                        SqlParameter cParam = new SqlParameter(keyName, SqlDbType.Bit);
                        cParam.Value = val.Value.AsBoolean();
                        DbCommand.Parameters.Add(cParam);
                    }
                    else if (val.Value.DataType == DataType.Date)
                    {
                        SqlParameter cParam = new SqlParameter(keyName, SqlDbType.DateTime);
                        cParam.Value = val.Value.AsDate();
                        DbCommand.Parameters.Add(cParam);
                    }
                    else if (val.Value.DataType == DataType.Number)
                    {
                        SqlParameter cParam = new SqlParameter(keyName, SqlDbType.Decimal);
                        cParam.Value = val.Value.AsNumber();
                        DbCommand.Parameters.Add(cParam);
                    }
                    else if (val.Value.DataType == DataType.String)
                    {
                        SqlParameter cParam = new SqlParameter(keyName, SqlDbType.NVarChar);
                        cParam.Value = val.Value.AsString();
                        DbCommand.Parameters.Add(cParam);
                    }
                    else
                    {
                        throw new NotSupportedException("Знасение этого типа не может быть переданно как значение в запрос к БД");
                    }
                }
            }

            SqlDataReader DbReader = DbCommand.ExecuteReader();

            ValueTable resultTable = new ValueTable();
            int fCount = DbReader.FieldCount;

            for (int i = 0; i < fCount; i++)
            {
                String fName = DbReader.GetName(i);
                resultTable.Columns.Add(fName);
            }


            while (DbReader.Read())
            {
                ValueTableRow row = resultTable.Add();
                for (int i = 0; i < fCount; i++)
                {

                    var col = DbReader.GetValue(i);

                    if (col is Boolean)
                    {
                        row.Set(i, ValueFactory.Create((bool)col));
                    }
                    else if (col is DateTime)
                    {
                        row.Set(i, ValueFactory.Create((DateTime)col));
                    }
                    else if (col is Decimal)
                    {
                        row.Set(i, ValueFactory.Create((Decimal)col));
                    }
                    else if (col is string)
                    {
                        row.Set(i, ValueFactory.Create((string)col));
                    }
                    else if (col is Int32)
                    {
                        row.Set(i, ValueFactory.Create(Convert.ToInt32(col)));
                    }
                    else
                    {
                        throw new NotSupportedException("Тип возвращенный из БД не поддерживаеться");
                    }


                }

            }

            DbReader.Close();
            DbCommand.Dispose();
            conn.Close();

            return resultTable;
        }

        #region IDisposable Support
        private bool disposedValue = false; // Для определения избыточных вызовов

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    // TODO: освободить управляемое состояние (управляемые объекты).
                }

                // TODO: освободить неуправляемые ресурсы (неуправляемые объекты) и переопределить ниже метод завершения.
                // TODO: задать большим полям значение NULL.

                disposedValue = true;
            }
        }

        // TODO: переопределить метод завершения, только если Dispose(bool disposing) выше включает код для освобождения неуправляемых ресурсов.
        // ~OSDB() {
        //   // Не изменяйте этот код. Разместите код очистки выше, в методе Dispose(bool disposing).
        //   Dispose(false);
        // }

        // Этот код добавлен для правильной реализации шаблона высвобождаемого класса.
        public void Dispose()
        {
            // Не изменяйте этот код. Разместите код очистки выше, в методе Dispose(bool disposing).
            Dispose(true);
            // TODO: раскомментировать следующую строку, если метод завершения переопределен выше.
            // GC.SuppressFinalize(this);
        }
        #endregion
    }
}
