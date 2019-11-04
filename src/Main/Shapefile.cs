using Microsoft.SqlServer.Types;
using Reimers.Map;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Odbc;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Xml;
using USC.GISResearchLab.Common.Core.KML;
using USC.GISResearchLab.Common.Databases.Odbc;
using USC.GISResearchLab.Common.Utils.Databases.TableDefinitions;


namespace Reimers.Esri
{
    /// <summary>
    /// Holds the data contained in an ESRI shapefile.
    /// </summary>
    public class Shapefile
    {
        #region Fields

        private DataTable dtPoint;
        private string strFile;
        private Stream fp;
        private BinaryReader br;
        private string strError = string.Empty;
        private GoogleBounds gbShape = new GoogleBounds();
        private int shp_type = 0;
        private List<ShapeRecord> lstRecords;

        // Kaveh: Streaming data out of shapefile
        private Stream shapeStream;
        private BinaryReader shapeReader;
        private OdbcDataReader fileDataReader;

        #endregion

        #region Events

        /// <summary>
        /// Triggered when the <see cref="Shapefile.ToKML"/> or <see cref="Shapefile.ToKmlStream"/> finishes writing.
        /// </summary>
        public event ShapefileHandler KmlWritten;

        /// <summary>
        /// Triggered when the <see cref="Shapefile.Shapefile2KmlStream"/> method reads an unknown <see cref="GoogleOverlay"/> type.
        /// </summary>
        public event OverlayWriter WriteCustomOverlay;

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
        /// Triggered when a record has been read from the shapefile.
        /// </summary>
        /// <remarks>
        /// <para>Tie an event handler to this event to be notified when a record has been read from the shapefile.</para>
        /// </remarks>
        public event ShapefileRecordReadHandler ShapefileRecordRead;

        /// <summary>
        /// Triggered when a record has been read from the dbf file.
        /// </summary>
        /// <remarks>
        /// <para>Tie an event handler to this event to be notified when a record has been read from the dbf.</para>
        /// </remarks>
        public event DbfRecordReadHandler DbfRecordRead;

        /// <summary>
        /// Triggered when the total number of records has been read from the dbf file.
        /// </summary>
        /// <remarks>
        /// <para>Tie an event handler to this event to be notified the total number of records has been read from the dbf.</para>
        /// </remarks>
        public event DbfNumberOfRecordsReadHandler DbfNumberOfRecordsRead;

        /// <summary>
        /// Triggered when a shape record has values computed.
        /// </summary>
        /// <remarks>
        /// <para>Tie an event handler to this event to be notified when a record has been read from the shapefile.</para>
        /// </remarks>
        public event ShapefileRecordConvertedHandler ShapefileRecordConverted;

        #endregion

        #region Constructor

        /// <summary>
        /// Creates a new instance of a Shapefile
        /// </summary>
        /// <param name="FileName">The path to the .shp file in the shapefile to open.</param>
        public Shapefile(string FileName)
        {
            if (System.IO.Path.GetExtension(FileName).ToLower() != ".shp")
            {
                throw new Exception("The filename must point to the .shp file in the shapefile");
            }

            strFile = FileName;
            fp = System.IO.File.OpenRead(strFile);
            br = new System.IO.BinaryReader(fp);
            BasicConfiguration();

            // Kaveh: Streaming data out of shapefile
            shapeReader = null;
            shapeStream = null;
            fileDataReader = null;
            DoCopyDBFInStreamMode = true;
            TempDbfFile = null;
        }

        ~Shapefile()
        {
            CloseStream();
        }

        #endregion

        #region Properties

        public bool DoCopyDBFInStreamMode { get; set; }

        private int _NotifyAfter;
        public int NotifyAfter
        {
            get { return _NotifyAfter; }
            set { _NotifyAfter = value; }
        }


        private int _NumberOfRecords;
        public int NumberOfRecords
        {
            get
            {
                return _NumberOfRecords;
            }
            set
            {
                _NumberOfRecords = value;
                if (DbfNumberOfRecordsRead != null)
                {
                    DbfNumberOfRecordsRead(_NumberOfRecords);
                }
            }
        }
        /// <summary>
        /// Kaveh: Returns the percentage of shaperecord bytes read from the steam. It will be a double number between 0 to 1.
        /// </summary>
        public double PercentShapesStreamed
        {
            get
            {
                if (shapeStream == null) return 0.0;
                else return Convert.ToDouble(shapeStream.Position) / shapeStream.Length;
            }
        }

        /// <summary>
        /// Gets the filename of the .shp file in the shapefile.
        /// </summary>
        public string FileName
        {
            get { return strFile; }
        }

        private string _TempDbfFile;
        public string TempDbfFile
        {
            get { return _TempDbfFile; }
            set { _TempDbfFile = value; }
        }

        /// <summary>
        /// Gets a list of <see cref="ShapeRecord"/> contained in the Shapefile.
        /// </summary>
        /// <remarks>
        /// <para>
        /// The <see cref="ShapeRecord"/> in the Shapefile are not parsed until the first time this method is called. 
        /// This can cause some delay when reading large shapefiles.
        /// </para>
        /// </remarks>
        public List<ShapeRecord> Records
        {
            get
            {
                if (lstRecords != null)
                {
                    return lstRecords;
                }

                lstRecords = new List<ShapeRecord>();
                fp.Seek(100, System.IO.SeekOrigin.Begin);

                while (fp.Position < fp.Length)
                {
                    ShapeRecord shp = new ShapeRecord(PointParsed, UnknownRecord);
                    shp.Read(br);

                    if (string.IsNullOrEmpty(shp.Error))
                    {
                        this.lstRecords.Add(shp);
                    }
                    else
                    {
                        throw new Exception(shp.Error);
                    }

                    if (ShapefileRecordRead != null)
                    {
                        if (NotifyAfter > 0)
                        {
                            if (lstRecords.Count % NotifyAfter == 0)
                            {
                                ShapefileRecordRead(lstRecords.Count);
                            }
                        }
                        else
                        {
                            ShapefileRecordRead(lstRecords.Count);
                        }
                    }
                }
                fp.Close();

                if (ShapefileRecordRead != null)
                {
                    ShapefileRecordRead(lstRecords.Count);
                }

                return lstRecords;
            }
        }

        private DataTable _ShapeFileDataTable;
        public DataTable ShapeFileDataTable
        {
            get { return _ShapeFileDataTable; }
            set { _ShapeFileDataTable = value; }
        }

        /// <summary>
        /// Gets a <see cref="DataTable"/> containing the .dbf file contents.
        /// </summary>
        /// 
        private DataTable _DbfDataTable;
        public DataTable DbfDataTable
        {
            get
            {
                if (_DbfDataTable != null)
                {
                    return _DbfDataTable;
                }
                else
                {
                    _DbfDataTable = new System.Data.DataTable();
                    string fn = string.Format(@"{0}\{1}.dbf", System.IO.Path.GetDirectoryName(FileName), System.IO.Path.GetFileNameWithoutExtension(FileName));

                    if (System.IO.File.Exists(fn))
                    {
                        string temp = Directory.GetCurrentDirectory() + "\\temp.dbf";

                        if (File.Exists(temp))
                        {
                            File.Delete(temp);
                        }

                        System.IO.File.SetAttributes(fn, FileAttributes.Normal);
                        System.IO.File.Copy(fn, temp, true);
                        File.SetAttributes(temp, FileAttributes.Temporary);

                        OdbcConnection conn = new OdbcConnection(string.Format(@"DBQ={0};Driver={{Microsoft dBase Driver (*.dbf)}}; DriverId=277;FIL=dBase4.0", Directory.GetCurrentDirectory()));
                        System.Diagnostics.Debug.WriteLine(System.IO.Path.GetDirectoryName(FileName));
                        OdbcCommand cmd = new OdbcCommand();
                        cmd.CommandText = "SELECT * FROM [temp.dbf]";
                        cmd.Connection = conn;
                        conn.Open();
                        OdbcDataReader dtr = cmd.ExecuteReader();

                        for (int i = 0; i < dtr.FieldCount; i++)
                        {
                            if (!_DbfDataTable.Columns.Contains(dtr.GetName(i)))
                            {
                                _DbfDataTable.Columns.Add(dtr.GetName(i), dtr.GetFieldType(i));
                            }
                        }

                        while (dtr.Read())
                        {
                            System.Data.DataRow dr = _DbfDataTable.NewRow();

                            for (int i = 0; i < _DbfDataTable.Columns.Count; i++)
                            {
                                dr[_DbfDataTable.Columns[i].ColumnName] = dtr[_DbfDataTable.Columns[i].ColumnName];
                            }

                            _DbfDataTable.Rows.Add(dr);

                            if (DbfRecordRead != null)
                            {
                                if (NotifyAfter > 0)
                                {
                                    if (_DbfDataTable.Rows.Count % NotifyAfter == 0)
                                    {
                                        DbfRecordRead(_DbfDataTable.Rows.Count);
                                    }
                                }
                                else
                                {
                                    DbfRecordRead(_DbfDataTable.Rows.Count);
                                }
                            }
                        }
                        dtr.Close();
                        conn.Close();

                        if (System.IO.File.Exists(temp))
                        {
                            System.IO.File.Delete(temp);
                        }

                        NumberOfRecords = _DbfDataTable.Rows.Count;
                    }

                    if (DbfRecordRead != null)
                    {
                        DbfRecordRead(_DbfDataTable.Rows.Count);
                    }

                    return _DbfDataTable;
                }
            }
        }

        public DataTable GetShapefileAsDataTable()
        {
            return GetShapefileAsDataTable(true, true, false, true, false, 4326);
        }

        public DataTable GetShapefileAsDataTable(bool includeSqlGeometry, bool includeSqlGeography, bool includeKML, bool includeWKT, bool includeLineString)
        {
            return GetShapefileAsDataTable(includeSqlGeometry, includeSqlGeography, includeKML, includeWKT, includeLineString, false, false, 4326);
        }

        public DataTable GetShapefileAsDataTable(bool includeSqlGeometry, bool includeSqlGeography, bool includeKML, bool includeWKT, bool includeLineString, int SRID)
        {
            return GetShapefileAsDataTable(includeSqlGeometry, includeSqlGeography, includeKML, includeWKT, includeLineString, false, false, SRID);
        }

        public DataTable GetShapefileAsDataTable(bool includeSqlGeometry, bool includeSqlGeography, bool includeKML, bool includeWKT, bool includeLineString, bool includeEndPointsWKT, bool includeEndPointsLineString, int SRID)
        {
            return GetShapefileAsDataTable(includeSqlGeometry, includeSqlGeography, includeKML, includeWKT, includeLineString, includeEndPointsWKT, includeEndPointsLineString, true, SRID);
        }
        public DataTable GetShapefileAsDataTable(bool includeSqlGeometry, bool includeSqlGeography, bool includeKML, bool includeWKT, bool includeLineString, bool includeEndPointsWKT, bool includeEndPointsLineString, bool includeShapeType, int SRID)
        {
            try
            {
                if (ShapeFileDataTable == null)
                {
                    string fn = string.Format(@"{0}\{1}.dbf", Path.GetDirectoryName(FileName), Path.GetFileNameWithoutExtension(FileName));
                    if (File.Exists(fn))
                    {

                        ShapeFileDataTable = DbfDataTable;

                        if (includeShapeType)
                        {
                            ShapeFileDataTable.Columns.Add("shapeType", typeof(String));
                        }

                        if (includeSqlGeometry)
                        {
                            ShapeFileDataTable.Columns.Add("shapeGeom", typeof(SqlGeometry));
                        }

                        if (includeSqlGeography)
                        {
                            ShapeFileDataTable.Columns.Add("shapeGeog", typeof(SqlGeography));
                        }

                        if (includeKML)
                        {
                            ShapeFileDataTable.Columns.Add("shapeKML", typeof(String));
                        }

                        if (includeWKT)
                        {
                            ShapeFileDataTable.Columns.Add("shapeWKT", typeof(String));
                        }

                        if (includeLineString)
                        {
                            ShapeFileDataTable.Columns.Add("shapeLineString", typeof(String));
                        }

                        if (includeEndPointsLineString)
                        {
                            ShapeFileDataTable.Columns.Add("fromLat", typeof(String));
                            ShapeFileDataTable.Columns.Add("fromLon", typeof(String));

                            ShapeFileDataTable.Columns.Add("toLat", typeof(String));
                            ShapeFileDataTable.Columns.Add("toLon", typeof(String));
                        }

                        if (includeEndPointsWKT)
                        {
                            ShapeFileDataTable.Columns.Add("shapeFromPointWKT", typeof(String));
                            ShapeFileDataTable.Columns.Add("shapeToPointWKT", typeof(String));
                        }


                        ShapeRecord[] list = Records.ToArray();

                        for (int j = 0; j < ShapeFileDataTable.Rows.Count; j++)
                        {
                            DataRow dataRow = ShapeFileDataTable.Rows[j];

                            SqlGeography sqlGeography = null;
                            SqlGeometry sqlGeometry = null;

                            foreach (GoogleOverlay ov in list[j].Overlays)
                            {
                                if (includeSqlGeometry)
                                {
                                    if (sqlGeometry == null)
                                    {
                                        sqlGeometry = ov.ToSqlGeometry("", SRID);
                                    }
                                    else
                                    {
                                        sqlGeometry = sqlGeometry.STUnion(ov.ToSqlGeometry("", SRID));
                                    }

                                    dataRow["shapeGeom"] = sqlGeometry;
                                }

                                if (includeSqlGeography)
                                {
                                    if (sqlGeography == null)
                                    {
                                        sqlGeography = ov.ToSqlGeography("", SRID);
                                    }
                                    else
                                    {
                                        sqlGeography = sqlGeography.STUnion(ov.ToSqlGeography("", SRID));
                                    }

                                    dataRow["shapeGeog"] = sqlGeography;
                                }

                                if (includeKML)
                                {
                                    string shapeWKTString = ov.ToWKT2D("");
                                    dataRow["shapeKML"] = ToKML(null, null, null);
                                }

                                if (includeWKT)
                                {
                                    string shapeWKTString = ov.ToWKT2D("");
                                    dataRow["shapeWKT"] = shapeWKTString;
                                }

                                if (includeLineString)
                                {
                                    string shapeLineString = ov.ToLineString2D("", ",");
                                    dataRow["shapeLineString"] = shapeLineString;
                                }

                                if (includeEndPointsLineString)
                                {
                                    string endPoints = ov.EndPointsLineString("", ",");
                                    string[] points = endPoints.Split(',');
                                    string startPoint = points[0];
                                    string endPoint = points[1];
                                    string[] startCoords = startPoint.Split(' ');
                                    string[] endCoords = endPoint.Split(' ');

                                    dataRow["fromLat"] = startCoords[0];
                                    dataRow["fromLon"] = startCoords[1];
                                    dataRow["toLat"] = endCoords[0];
                                    dataRow["toLon"] = endCoords[1];
                                }

                                if (includeEndPointsWKT)
                                {
                                    string endPoints = ov.EndPointsWKT("", ",");
                                    string[] points = endPoints.Split(',');
                                    string startPoint = points[0];
                                    string endPoint = points[1];
                                    dataRow["shapeFromPointWKT"] = startPoint;
                                    dataRow["shapeToPointWKT"] = endPoint;
                                }

                                string shapeType = "";

                                if (ov is GoogleMarker)
                                {
                                    shapeType = KMLGeometryTypes.GetKMLGeometryTypeName(KMLGeometryType.Point);
                                }
                                else if (ov is GooglePolygon)
                                {
                                    shapeType = KMLGeometryTypes.GetKMLGeometryTypeName(KMLGeometryType.Polygon);
                                }
                                else if (ov is GooglePolyline)
                                {
                                    shapeType = KMLGeometryTypes.GetKMLGeometryTypeName(KMLGeometryType.LineString);
                                }
                                else
                                {
                                    throw new Exception("Unexpected or implemented Overlay GeometryType: " + ov.GetType());
                                }

                                if (includeShapeType)
                                {
                                    dataRow["shapeType"] = shapeType;
                                }
                            }

                            if (ShapefileRecordConverted != null)
                            {
                                if (NotifyAfter > 0)
                                {
                                    if (j % NotifyAfter == 0)
                                    {
                                        ShapefileRecordConverted(j);
                                    }
                                }
                                else
                                {
                                    ShapefileRecordConverted(j);
                                }
                            }

                        }

                        if (ShapefileRecordConverted != null)
                        {
                            ShapefileRecordConverted(ShapeFileDataTable.Rows.Count);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                throw new Exception("Exception occurred GetShapefileAsDataTable: " + e.Message, e);
            }
            return ShapeFileDataTable;
        }


        /// <summary>
        /// Added by Dan - Gets a <see cref="DataTable"/> containing the .dbf file contents as well as columns containing ShapeType and SqlGeometry.
        /// </summary>
        public DataTable ShapefileDataTableWithSqlGeometry
        {
            get { return GetShapefileAsDataTable(true, false, false, false, false); }
        }

        /// <summary>
        /// Added by Dan - Gets a <see cref="DataTable"/> containing the .dbf file contents as well as columns containing ShapeType and SqlGeography.
        /// </summary>
        public DataTable ShapefileDataTableWithSqlGeography
        {
            get { return GetShapefileAsDataTable(false, true, false, false, false); }
        }


        /// <summary>
        /// Added by Dan - Gets a <see cref="DataTable"/> containing the .dbf file contents as well as columns containing ShapeType and ShapeKML.
        /// </summary>
        public DataTable ShapefileDataTableWithKMLGeometry
        {
            get { return GetShapefileAsDataTable(false, false, true, false, false); }
        }

        /// <summary>
        /// Added by Dan - Gets a <see cref="DataTable"/> containing the .dbf file contents as well as columns containing ShapeType and ShapeLineString.
        /// </summary>
        public DataTable ShapefileDataTableWithLineString
        {
            get { return GetShapefileAsDataTable(false, false, false, false, true); }
        }

        /// <summary>
        /// Added by Dan - Gets a <see cref="DataTable"/> containing the .dbf file contents as well as columns containing ShapeType and ShapeLineString.
        /// </summary>
        public DataTable ShapefileDataTableWithWKTString
        {
            get { return GetShapefileAsDataTable(false, false, false, true, false); }
        }

        /// <summary>
        /// Gets any error messages for the <see cref="Shapefile"/>.
        /// </summary>        
        public string Error
        {
            get { return strError; }
        }

        #endregion

        #region Methods

        #region Public

        /// <summary>
        /// Kaveh: Simply points to the DBF file and using the common.database function, returns the table schema (non-spatial data)
        /// </summary>
        /// <returns>An array of TableColumns containing the schema information of the shapefile (non-spatial data)</returns>
        public TableColumn[] GetDBFSchema()
        {
            TableColumn[] schemaCols = null;

            try
            {
                if (File.Exists(FileName))
                {
                    var DbfFile = GetDBFFileName(FileName);

                    var odbcMan = new OdbcSchemaManager("DBQ=" + Path.GetDirectoryName(DbfFile) + "\\;Driver={Microsoft dBASE Driver (*.dbf)};DriverId=277;FIL=dBase4.0");
                    schemaCols = odbcMan.GetColumns(Path.GetFileNameWithoutExtension(DbfFile));
                }
            }
            catch (Exception e)
            {
                throw new Exception("Exception in GetDBFSchema: " + e.Message, e);
            }
            finally
            {
                if (!String.IsNullOrEmpty(TempDbfFile) && File.Exists(TempDbfFile)) File.Delete(TempDbfFile);
            }
            return schemaCols;
        }

        /// <summary>
        /// Closes the stream objects and release all resources
        /// </summary>
        public void CloseStream()
        {
            if (shapeStream != null) shapeStream.Close();

            if (fileDataReader != null) fileDataReader.Close();

            if (!String.IsNullOrEmpty(TempDbfFile) && File.Exists(TempDbfFile))
            {
                File.Delete(TempDbfFile);
                TempDbfFile = null;
            }
        }

        /// <summary>
        /// Resets the stream pointers to the begining of the file
        /// </summary>
        public void ResetNextFeature()
        {
            if (File.Exists(FileName))
            {
                shapeStream = File.OpenRead(strFile);
                shapeReader = new BinaryReader(shapeStream);
                shapeStream.Seek(100, SeekOrigin.Begin);

                string DbfFile = GetDBFFileName(FileName);

                OdbcConnection conn = new OdbcConnection("DBQ=" + Path.GetDirectoryName(DbfFile) + "\\;Driver={Microsoft dBASE Driver (*.dbf)};DriverId=277;FIL=dBase4.0");
                OdbcCommand cmd = new OdbcCommand();
                cmd.CommandText = "SELECT * FROM [" + Path.GetFileNameWithoutExtension(DbfFile) + "]";
                cmd.Connection = conn;
                conn.Open();

                fileDataReader = cmd.ExecuteReader();
            }
        }


        /// <summary>
        /// Kaveh: Advances the shapefile file pointers to the next record and returns the associated shape and dbf data right from the file
        /// </summary>
        /// <returns>A new copy of the next shapefile record</returns>
        public ShapefileRecord NextFeature()
        {
            ShapefileRecord ret = null;

            if ((shapeStream == null) || (fileDataReader == null)) ResetNextFeature();

            if ((shapeStream.Position < shapeStream.Length) && (fileDataReader.HasRows))
            {
                ret = new ShapefileRecord();
                ret.SteamedBytesRatio = shapeStream.Position;
                ret.Shape = new ShapeRecord(PointParsed, UnknownRecord);
                ret.Shape.Read(shapeReader);
                ret.SteamedBytesRatio = (shapeStream.Position - ret.SteamedBytesRatio) / shapeStream.Length;

                if (!string.IsNullOrEmpty(ret.Shape.Error)) throw new Exception(ret.Shape.Error);

                ret.DataArray = new object[fileDataReader.FieldCount];
                if (fileDataReader.Read()) fileDataReader.GetValues(ret.DataArray);
            }
            else
            {
                // end of file reached
                CloseStream();
            }
            return ret;
        }

        /// <summary>
        /// Writes the Shapefile to the specified file.
        /// </summary>
        /// <param name="Filename">The path to the file where the KML markup will be written.</param>
        /// <param name="FolderName">The name of the folder containing the overlays in the KML markup.</param>
        /// <param name="NameColumn">
        /// <para>The column of the .dbf file table to use as the name of the <see cref="GoogleOverlay"/>.</para>
        /// <para>This value can be null or empty.</para>
        /// </param>
        /// <param name="MetaColumns">An array of strings giving the names of the columns to write to the description of the <see cref="GoogleOverlay"/>.</param>
        public void ToKmlFile(string Filename, string FolderName, string NameColumn, string[] MetaColumns)
        {
            FileStream fs = new FileStream(Filename, FileMode.OpenOrCreate, FileAccess.Write, FileShare.Read);
            ToKmlStream(fs, FolderName, NameColumn, MetaColumns, true);
        }

        /// <summary>
        /// Writes the Shapefile to the specified <see cref="Stream"/>.
        /// </summary>
        /// <param name="KmlStream">The <see cref="Stream"/> to write the KML data to.</param>
        /// <param name="FolderName">The name of the folder containing the overlays in the KML markup.</param>
        /// <param name="NameColumn">
        /// <para>The column of the .dbf file table to use as the name of the <see cref="GoogleOverlay"/>.</para>
        /// <para>This value can be null or empty.</para>
        /// </param>
        /// <param name="MetaColumns">An array of strings giving the names of the columns to write to the description of the <see cref="GoogleOverlay"/>.</param>
        /// <param name="CloseStream">Sets whether the <see cref="Stream"/> should be closed after writing.</param>
        public void ToKmlStream(Stream KmlStream, string FolderName, string NameColumn, string[] MetaColumns, bool CloseStream)
        {
            XmlTextWriter wXML = new XmlTextWriter(KmlStream, Encoding.UTF8);
            wXML.WriteStartDocument();
            wXML.WriteStartElement("kml", "http://earth.google.com/kml/2.0");
            wXML.WriteStartElement("Folder");
            wXML.WriteElementString("name", FolderName);
            wXML.WriteElementString("open", "1");
            wXML.Flush();
            for (int i = 0; i < Records.Count; i++)
            {
                foreach (GoogleOverlay ov in Records[i].Overlays)
                {
                    if (ov is GoogleMarker)
                    {
                        GoogleMarker m = (GoogleMarker)ov;
                        if (!string.IsNullOrEmpty(NameColumn)) m.Options.Name = GetColumnValue(i, NameColumn);
                        if (MetaColumns != null) m.Options.Description = GetRecordData(i, MetaColumns);
                        wXML.WriteRaw(m.ToKML(FolderName, false));
                        wXML.Flush();
                        m = null;
                    }
                    else if (ov is GooglePolygon)
                    {
                        GooglePolygon p = (GooglePolygon)ov;
                        if (!string.IsNullOrEmpty(NameColumn)) p.Options.Name = GetColumnValue(i, NameColumn);
                        if (MetaColumns != null) p.Options.Description = GetRecordData(i, MetaColumns);
                        wXML.WriteRaw(p.ToKML(FolderName, false));
                        wXML.Flush();
                        p = null;
                    }
                    else if (ov is GooglePolyline)
                    {
                        GooglePolyline p = (GooglePolyline)ov;
                        if (!string.IsNullOrEmpty(NameColumn)) p.Options.Name = GetColumnValue(i, NameColumn);
                        if (MetaColumns != null) p.Options.Description = GetRecordData(i, MetaColumns);
                        wXML.WriteRaw(p.ToKML(FolderName, false));
                        wXML.Flush();
                        p = null;
                    }
                    else
                    {
                        if (WriteCustomOverlay != null && DbfDataTable != null)
                        {
                            wXML.WriteRaw(WriteCustomOverlay(ov, DbfDataTable.Rows[i].ItemArray));
                            wXML.Flush();
                        }
                    }
                }
            }
            wXML.WriteEndElement();
            wXML.WriteEndElement();
            wXML.WriteEndDocument();
            wXML.Flush();
            if (CloseStream) wXML.Close();
            if (KmlWritten != null) KmlWritten();
        }

        /// <summary>
        /// Writes the specified file directly to the specified stream.
        /// </summary>
        /// <param name="Shapefile">The path to the .shp file of the shapefile to write.</param>
        /// <param name="KmlStream">The <see cref="Stream"/> to output the KML to.</param>
        /// <param name="FolderName">The name of the folder containing the overlays in the KML markup.</param>
        /// <param name="NameColumn">
        /// <para>The column of the .dbf file table to use as the name of the <see cref="GoogleOverlay"/>.</para>
        /// <para>This value can be null or empty.</para>
        /// </param>
        /// <param name="MetaColumns">An array of strings giving the names of the columns to write to the description of the <see cref="GoogleOverlay"/>.</param>
        /// <param name="CloseStream">Sets whether the <see cref="Stream"/> should be closed after writing.</param>
        public static void Shapefile2KmlStream(string Shapefile, Stream KmlStream, string FolderName, string NameColumn, string[] MetaColumns, bool CloseStream)
        {
            Reimers.Esri.Shapefile f = new Shapefile(Shapefile);
            f.ToKmlStream(KmlStream, FolderName, NameColumn, MetaColumns, CloseStream);
        }

        /// <summary>
        /// Writes the specified file directly to the specified file.
        /// </summary>
        /// <param name="Shapefile">The path to the .shp file of the shapefile to write.</param>
        /// <param name="KmlFile">
        /// <para>The path to the file to write to.</para>
        /// <para>If the file does not exist it will be created.</para>
        /// </param>
        /// <param name="FolderName">The name of the folder containing the overlays in the KML markup.</param>
        /// <param name="NameColumn">
        /// <para>The column of the .dbf file table to use as the name of the <see cref="GoogleOverlay"/>.</para>
        /// <para>This value can be null or empty.</para>
        /// </param>
        /// <param name="MetaColumns">An array of strings giving the names of the columns to write to the description of the <see cref="GoogleOverlay"/>.</param>
        public static void Shapefile2KmlFile(string Shapefile, string KmlFile, string FolderName, string NameColumn, string[] MetaColumns)
        {
            FileStream fs = new FileStream(KmlFile, FileMode.OpenOrCreate, FileAccess.Write, FileShare.Read);
            Shapefile2KmlStream(Shapefile, fs, FolderName, NameColumn, MetaColumns, true);
        }

        /// <summary>
        /// Writes the Shapefile to a KML formatted string.
        /// </summary>
        /// <param name="Folder">The name of the folder containing the overlays in the KML markup.</param>
        /// <param name="Name">
        /// <para>The column of the .dbf file table to use as the name of the <see cref="GoogleOverlay"/>.</para>
        /// <para>This value can be null or empty.</para>
        /// </param>
        /// <param name="Columns">An array of strings giving the names of the columns to write to the description of the <see cref="GoogleOverlay"/>.</param>
        /// <returns>A KML formatted string representation of the Shapefile.</returns>
        public string ToKML(string Folder, string Name, string[] Columns)
        {
            GoogleOverlayCollection goc = new GoogleOverlayCollection();
            for (int i = 0; i < Records.Count; i++)
            {
                foreach (GoogleOverlay ov in Records[i].Overlays)
                {
                    string ss = string.Empty;
                    if (ov is GoogleMarker)
                    {
                        GoogleMarker m = (GoogleMarker)ov;
                        m.Options.Description = GetRecordData(i, Columns);
                        if (!string.IsNullOrEmpty(Name)) m.Options.Name = DbfDataTable.Rows[i][Name].ToString();
                        Debug.WriteLine(DbfDataTable.Rows[i][Name].ToString());
                        goc.Add(m);
                    }
                    else if (ov is GooglePolyline)
                    {
                        GooglePolyline l = (GooglePolyline)ov;
                        l.Options.Description = GetRecordData(i, Columns);
                        if (!string.IsNullOrEmpty(Name)) l.Options.Name = DbfDataTable.Rows[i][Name].ToString();
                        goc.Add(l);
                    }
                    else if (ov is GooglePolygon)
                    {
                        GooglePolygon p = (GooglePolygon)ov;
                        p.Options.Description = GetRecordData(i, Columns);
                        if (!string.IsNullOrEmpty(Name)) p.Options.Name = DbfDataTable.Rows[i][Name].ToString();
                        goc.Add(p);
                    }
                }
            }
            if (KmlWritten != null) KmlWritten();
            return goc.ToKML(Folder);
        }

        #endregion

        #region Private

        private string GetDBFFileName(string DbfFile)
        {
            string fn = string.Format(@"{0}\{1}.dbf", Path.GetDirectoryName(FileName), Path.GetFileNameWithoutExtension(FileName));
            if (DoCopyDBFInStreamMode)
            {
                // we need to rename to a temp file in case the dbf file (and shapefile) are longer than 10 chars
                // this file is deleted in the destructor, but we delete here as well for good measure
                if (File.Exists(fn))
                {
                    DbfFile = Directory.GetCurrentDirectory() + "\\temp.dbf";

                    if (!String.IsNullOrEmpty(TempDbfFile) && File.Exists(TempDbfFile))
                    {
                        File.Delete(TempDbfFile);
                        TempDbfFile = null;
                    }

                    TempDbfFile = DbfFile;

                    if (File.Exists(DbfFile)) File.Delete(DbfFile);

                    File.SetAttributes(fn, FileAttributes.Normal);
                    File.Copy(fn, DbfFile, true);
                    File.SetAttributes(DbfFile, FileAttributes.Temporary);
                }
            }
            else
            {
                // get the 8.3 dos name refresentation
                //if (Path.GetFileNameWithoutExtension(fn).Length > 8) DbfFile = FileUtils.GetShortFileName(fn);
                throw new Exception("GetShortFileName Function missing");
            }
            return DbfFile;
        }

        public GoogleBounds ParseBoundingBox(System.IO.BinaryReader r)
        {
            GoogleBounds data = new GoogleBounds();
            data.MinLongitude = r.ReadDouble();
            data.MinLatitude = r.ReadDouble();
            data.MaxLongitude = r.ReadDouble();
            data.MaxLatitude = r.ReadDouble();

            return data;
        }

        private void BasicConfiguration()
        {
            fp.Seek(32, System.IO.SeekOrigin.Begin);
            shp_type = br.ReadInt32();

            gbShape = ParseBoundingBox(br);
        }

        public string GetColumnValue(int Index, string ColumnName)
        {
            if (DbfDataTable == null) return string.Empty;
            return DbfDataTable.Rows[Index][ColumnName].ToString();
        }

        public string GetRecordData(int Index, string[] Columns)
        {
            StringBuilder sb = new StringBuilder();
            if (Columns == null)
            {
                for (int j = 0; j < DbfDataTable.Columns.Count; j++)
                {
                    sb.Append(DbfDataTable.Rows[Index][j].ToString());
                    if (j < DbfDataTable.Columns.Count - 1) sb.Append("|");
                }
            }
            else
            {
                for (int j = 0; j < Columns.Length; j++)
                {
                    sb.Append(DbfDataTable.Rows[Index][Columns[j]].ToString());
                    if (j < Columns.Length - 1) sb.Append("|");
                }
            }
            return sb.ToString();
        }

        #endregion

        #endregion
    }
}