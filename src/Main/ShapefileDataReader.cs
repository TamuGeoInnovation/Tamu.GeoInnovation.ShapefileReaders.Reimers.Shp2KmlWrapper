using Microsoft.SqlServer.Types;
using Reimers.Map;
using System;
using System.Collections;
using System.Data;
using System.Data.Odbc;
using System.IO;
using USC.GISResearchLab.Common.Core.KML;
using USC.GISResearchLab.Common.Databases.DataReaders;

namespace Reimers.Esri
{
    /// <summary>
    /// Holds the data contained in an ESRI shapefile.
    /// </summary>
    public class ShapefileDataReader : AbstractDataReader
    {
        #region Fields

        private Stream fp;
        private BinaryReader br;
        private int shp_type = 0;

        public string strError { get; set; }
        public GoogleBounds GoogleBoundsShape { get; set; }

        public BinaryReader ShapeReader { get; set; }
        public Stream ShapeStream { get; set; }

        //public new OleDbConnection FileDataConnection { get; set; }
        //public new OleDbDataReader FileDataReader { get; set; }
        public new OdbcConnection FileDataConnection { get; set; }
        public new OdbcDataReader FileDataReader { get; set; }

        #endregion

        #region Events


        /// <summary>
        /// Triggered when an unknown shape is encountered in a shapfile definition.
        /// </summary>
        /// <remarks>
        /// <para>Tie an event handler to this event to handle reading of the unknown record.</para>
        /// </remarks>
        public event ReadUnkownShape UnknownRecord;

        /// <summary>
        /// Triggered when a point has been parsed in the shapefile definition.
        /// </summary>
        /// <remarks>
        /// <para>Tie an event handler to this event to handle custom conversions from coordinate types other than WGS84. The arguments are the raw numbers read in the shapefile.</para>
        /// </remarks>
        public event PointParsedHandler PointParsed;

        /// <summary>
        /// Triggered when a record has been read from the stream.
        /// </summary>
        /// <remarks>
        /// <para>Tie an event handler to this event to be notified when a record has been read from the stream.</para>
        /// </remarks>
        public event PercentReadHandler PercentRead;


        /// <summary>
        /// Triggered when a record has been read from the stream.
        /// </summary>
        /// <remarks>
        /// <para>Tie an event handler to this event to be notified when a record has been read from the stream.</para>
        /// </remarks>
        public event RecordsReadHandler RecordsRead;

        #endregion

        #region Properties

        public Hashtable ColumnOrdinalHashTable { get; set; }
        public bool IncludeSqlGeometry { get; set; }
        public bool IncludeSqlGeography { get; set; }
        public DataTable CurrentDataTable { get; set; }
        public int SRID { get; set; }
        public ShapefileRecord CurrentShapefileRecord { get; set; }

        public override object[] CurrentRow
        {
            get
            {
                object[] ret = null;
                if (CurrentShapefileRecord != null)
                {
                    ret = CurrentShapefileRecord.DataArray;
                }
                return ret;
            }
        }



        /// <summary>
        /// Kaveh: Returns the percentage of shaperecord bytes read from the steam. It will be a double number between 0 to 1.
        /// </summary>
        public double PercentShapesStreamed
        {
            get
            {
                if (ShapeStream == null) return 0.0;
                else return Convert.ToDouble(ShapeStream.Position) / ShapeStream.Length;
            }
        }

        /// <summary>
        /// Gets any error messages for the <see cref="Shapefile"/>.
        /// </summary>        
        public string Error
        {
            get { return strError; }
        }

        #endregion

        #region Constructor

        public ShapefileDataReader()
        {
            GoogleBoundsShape = new GoogleBounds();
            strError = string.Empty;
        }

        /// <summary>
        /// Creates a new instance of a Shapefile
        /// </summary>
        /// <param name="FileName">The path to the .shp file in the shapefile to open.</param>
        public ShapefileDataReader(string fileName)
        {

            FileName = fileName;

            if (Path.GetExtension(FileName).ToLower() != ".shp")
            {
                throw new Exception("The filename must point to the .shp file in the shapefile");
            }

            fp = File.OpenRead(FileName);
            br = new BinaryReader(fp);
            BasicConfiguration();

            ShapeReader = null;
            ShapeStream = null;
            FileDataReader = null;

            GoogleBoundsShape = new GoogleBounds();
            strError = string.Empty;
        }

        #endregion



        #region Methods

        #region Public

        /// <summary>
        /// Closes the stream objects and release all resources
        /// </summary>
        public void CloseStream()
        {
            if (ShapeStream != null)
            {
                ShapeStream.Close();
            }

            if (FileDataReader != null)
            {
                FileDataReader.Close();
            }

            if (FileDataConnection != null)
            {
                if (FileDataConnection.State != ConnectionState.Closed)
                {
                    FileDataConnection.Close();
                }
            }

            if (br != null)
            {
                br.Close();
            }

            if (fp != null)
            {
                fp.Close();
            }

            if (File.Exists(TempDfbFile))
            {
                if (File.Exists(TempDfbFile))
                {
                    File.Delete(TempDfbFile);
                }
            }

            IsClosed = true;
        }

        /// <summary>
        /// Resets the stream pointers to the begining of the file
        /// </summary>
        public void ResetNextFeature()
        {
            if (File.Exists(FileName))
            {
                ShapeStream = File.OpenRead(FileName);
                ShapeReader = new BinaryReader(ShapeStream);
                ShapeStream.Seek(100, SeekOrigin.Begin);

                if (FileDataConnection == null)
                {
                    FileDataConnection = new OdbcConnection("DBQ=" + Path.GetDirectoryName(FileName) + "\\;Driver={Microsoft dBASE Driver (*.dbf)};DriverId=277;FIL=dBase4.0");

                    OdbcCommand cmd = new OdbcCommand();
                    cmd.CommandText = "SELECT * FROM [" + Path.GetFileName(FileName) + "]";
                    cmd.Connection = FileDataConnection;
                    FileDataConnection.Open();
                    FileDataReader = cmd.ExecuteReader();

                    // use oleDb
                    //FileDataConnection = new OleDbConnection("DBQ=" + Path.GetDirectoryName(FileName) + "\\;Driver={Microsoft dBASE Driver (*.dbf)};DriverId=277;FIL=dBase4.0");

                    //OleDbCommand cmd = new OleDbCommand();
                    //cmd.CommandText = "SELECT * FROM [" + Path.GetFileName(FileName) + "]";
                    //cmd.Connection = FileDataConnectionOleDb;
                    //FileDataConnection.Open();
                    //FileDataReader = cmd.ExecuteReader();
                }
            }
        }

        /// <summary>
        /// Kaveh: Advances the shapefile file pointers to the next record and returns the associated shape and dbf data right from the file
        /// </summary>
        /// <returns>A new copy of the next shapefile record</returns>
        public override bool NextFeature()
        {
            bool ret = true;
            try
            {

                CurrentShapefileRecord = null;

                if ((ShapeStream == null) || (FileDataReader == null))
                {
                    ResetNextFeature();
                }

                if ((ShapeStream.Position < ShapeStream.Length) && (FileDataReader.HasRows))
                {

                    CurrentRecordIndex++;

                    if (RecordsRead != null)
                    {
                        if (NotifyAfter > 0)
                        {
                            if (CurrentRecordIndex % NotifyAfter == 0)
                            {
                                RecordsRead(CurrentRecordIndex, TotalRecordCount);
                            }
                        }
                        else
                        {
                            RecordsRead(CurrentRecordIndex, TotalRecordCount);
                        }
                    }

                    if (PercentRead != null)
                    {
                        if (NotifyAfter > 0)
                        {
                            if (CurrentRecordIndex % NotifyAfter == 0)
                            {
                                PercentRead(PercentShapesStreamed);
                            }
                        }
                        else
                        {
                            PercentRead(PercentShapesStreamed);
                        }
                    }

                    CurrentShapefileRecord = new ShapefileRecord();
                    CurrentShapefileRecord.SteamedBytesRatio = ShapeStream.Position;
                    CurrentShapefileRecord.Shape = new ShapeRecord(PointParsed, UnknownRecord);
                    CurrentShapefileRecord.Shape.Read(ShapeReader);
                    CurrentShapefileRecord.SteamedBytesRatio = (ShapeStream.Position - CurrentShapefileRecord.SteamedBytesRatio) / ShapeStream.Length;

                    if (!string.IsNullOrEmpty(CurrentShapefileRecord.Shape.Error))
                    {
                        throw new Exception(CurrentShapefileRecord.Shape.Error);
                    }

                    CurrentShapefileRecord.DataArray = new object[FileDataReader.FieldCount];
                    if (FileDataReader.Read())
                    {
                        FileDataReader.GetValues(CurrentShapefileRecord.DataArray);
                    }

                    //for (int i = 0; i < CurrentShapefileRecord.DataArray.Length; i++)
                    //{
                    //    CurrentRow[i] = CurrentShapefileRecord.DataArray[i];
                    //}

                    object[] temp = new object[CurrentShapefileRecord.DataArray.Length + 1];
                    Array.Copy(CurrentShapefileRecord.DataArray, temp, CurrentShapefileRecord.DataArray.Length);

                    if (CurrentShapefileRecord.Shape.ShapeType == ShapeType.Point)
                    {
                        temp[temp.Length - 1] = KMLGeometryTypes.GetKMLGeometryTypeName(KMLGeometryType.Point);
                    }
                    else if (CurrentShapefileRecord.Shape.ShapeType == ShapeType.Polygon)
                    {
                        temp[temp.Length - 1] = KMLGeometryTypes.GetKMLGeometryTypeName(KMLGeometryType.Polygon);
                    }
                    else if (CurrentShapefileRecord.Shape.ShapeType == ShapeType.PolyLine)
                    {
                        temp[temp.Length - 1] = KMLGeometryTypes.GetKMLGeometryTypeName(KMLGeometryType.LineString);
                    }

                    CurrentShapefileRecord.DataArray = temp;

                    //string name = null;
                    //if (GetOrdinal("name00") >= 0)
                    //{
                    //    name = (string)GetValue(GetOrdinal("name00"));
                    //}

                    if (IncludeSqlGeography)
                    {
                        temp = new object[CurrentShapefileRecord.DataArray.Length + 1];
                        Array.Copy(CurrentShapefileRecord.DataArray, temp, CurrentShapefileRecord.DataArray.Length);
                        temp[temp.Length - 1] = CurrentShapefileRecord.Shape.ToUnionSqlGeography("", SRID);
                        CurrentShapefileRecord.DataArray = temp;
                    }

                    if (IncludeSqlGeometry)
                    {
                        temp = new object[CurrentShapefileRecord.DataArray.Length + 1];
                        Array.Copy(CurrentShapefileRecord.DataArray, temp, CurrentShapefileRecord.DataArray.Length);
                        temp[temp.Length - 1] = CurrentShapefileRecord.Shape.ToUnionSqlGeometry("", SRID);
                        CurrentShapefileRecord.DataArray = temp;
                    }

                }
                else
                {
                    ret = false;
                    CloseStream();
                }
            }
            catch (Exception e)
            {
                throw new Exception("Exception occured in NextFeature: " + e.Message, e);
            }
            return ret;
        }

        public void BasicConfiguration()
        {
            fp.Seek(32, System.IO.SeekOrigin.Begin);
            shp_type = br.ReadInt32();

            GoogleBoundsShape = ParseBoundingBox(br);
        }

        #endregion

        #region Private

        public GoogleBounds ParseBoundingBox(System.IO.BinaryReader r)
        {
            GoogleBounds data = new GoogleBounds();
            data.MinLongitude = r.ReadDouble();
            data.MinLatitude = r.ReadDouble();
            data.MaxLongitude = r.ReadDouble();
            data.MaxLatitude = r.ReadDouble();

            return data;
        }

        #endregion

        #endregion

        #region IDataReader Members


        public override void Close()
        {
            CloseStream();
            base.Close();
        }

        public override DataTable GetSchemaTable()
        {

            try
            {
                if (SchemaTable == null)
                {

                    string fn = string.Format(@"{0}\{1}.dbf", Path.GetDirectoryName(FileName), Path.GetFileNameWithoutExtension(FileName));
                    if (File.Exists(fn))
                    {
                        int bits = IntPtr.Size * 8;

                        if (bits != 32)
                        {
                            throw new Exception("ExtendedShapefileDataReader must be run in 32-Bit mode due to dbf reading components. Recompile as 32-Bit or use the 32-Bit Binaries");
                        }
                        else
                        {
                            TempDfbFile = Directory.GetCurrentDirectory() + "\\temp.dbf";
                            File.SetAttributes(fn, FileAttributes.Normal);
                            File.Copy(fn, TempDfbFile, true);
                            File.SetAttributes(TempDfbFile, FileAttributes.Temporary);

                            FileDataConnection = new OdbcConnection(string.Format(@"DBQ={0};Driver={{Microsoft dBase Driver (*.dbf)}}; DriverId=277;FIL=dBase4.0", Directory.GetCurrentDirectory()));
                            FileDataConnection.Open();

                            OdbcCommand cmd = new OdbcCommand();
                            cmd.CommandText = "SELECT count(*) FROM [temp.dbf]";
                            cmd.Connection = FileDataConnection;
                            TotalRecordCount = Convert.ToInt32(cmd.ExecuteScalar());

                            cmd = new OdbcCommand();
                            cmd.CommandText = "SELECT * FROM [temp.dbf]";
                            cmd.Connection = FileDataConnection;
                            FileDataReader = cmd.ExecuteReader();
                            SchemaTable = FileDataReader.GetSchemaTable();


                            // use oleDb
                            //FileDataConnection = new OleDbConnection(string.Format(@"Provider=Microsoft.Jet.OLEDB.4.0;Extended Properties=dBASE IV;User ID=;Password=;Data Source={0};", Directory.GetCurrentDirectory()));
                            //FileDataConnection.Open();

                            //OleDbCommand cmd = new OleDbCommand();
                            //cmd.CommandText = "SELECT count(*) FROM [temp.dbf]";
                            //cmd.Connection = FileDataConnection;
                            //TotalRecordCount = Convert.ToInt32(cmd.ExecuteScalar());

                            //cmd = new OleDbCommand();
                            //cmd.CommandText = "SELECT * FROM [temp.dbf]";
                            //cmd.Connection = FileDataConnection;
                            //FileDataReader = cmd.ExecuteReader();
                            //SchemaTable = FileDataReader.GetSchemaTable();
                        }
                    }


                    if (SchemaTable != null)
                    {

                        DataRow row = SchemaTable.NewRow();
                        row["ColumnName"] = "shapeType";
                        row["ColumnOrdinal"] = 0;
                        row["DataType"] = typeof(string);
                        //row["ProviderType"] = 0;
                        row["IsReadOnly"] = true;
                        SchemaTable.Rows.Add(row);


                        if (IncludeSqlGeography)
                        {
                            row = SchemaTable.NewRow();
                            row["ColumnName"] = "shapeGeog";
                            row["ColumnOrdinal"] = 0;
                            row["DataType"] = typeof(SqlGeography);
                            //row["ProviderType"] = 0;
                            row["IsReadOnly"] = true;
                            SchemaTable.Rows.Add(row);
                        }

                        if (IncludeSqlGeometry)
                        {
                            row = SchemaTable.NewRow();
                            row["ColumnName"] = "shapeGeom";
                            row["ColumnOrdinal"] = 0;
                            row["DataType"] = typeof(SqlGeometry);
                            //row["ProviderType"] = typeof(SqlGeometry);
                            row["IsReadOnly"] = true;
                            SchemaTable.Rows.Add(row);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                throw new Exception("Exception occured GetSchemaTable: " + e.Message, e);
            }

            return SchemaTable;
        }

        #endregion


    }
}