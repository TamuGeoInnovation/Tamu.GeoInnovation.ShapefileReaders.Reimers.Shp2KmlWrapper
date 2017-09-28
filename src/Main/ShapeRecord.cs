using Microsoft.SqlServer.Types;
using Reimers.Map;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;

namespace Reimers.Esri
{
	/// <summary>
	/// Holds information about individual shape records found in a <see cref="Shapefile"/>.
	/// </summary>
	public class ShapeRecord
	{
		#region Fields

		private string strError;
		private List<GoogleOverlay> lst = new List<GoogleOverlay>();
		private BinaryReader br;
		private int intRecord = 0;
		private int intLength = 0;
		private ShapeType stType = 0;
		PointParsedHandler pc = null;
        ReadUnkownShape rus = null;

		#endregion

		#region Constructor

		/// <summary>
		/// Creates a new instance of a ShapeRecord.
		/// </summary>
		public ShapeRecord() { }

		/// <summary>
		/// Creates a new instance of a ShapeRecord.
		/// </summary>
		/// <param name="PointConverter">The delegate to handle Point Conversion.</param>
		/// <param name="UnknownShapeReader">The delegate to handle reading unknown shapes.</param>
		public ShapeRecord(PointParsedHandler PointConverter, ReadUnkownShape UnknownShapeReader)
		{
			pc = PointConverter;
			rus = UnknownShapeReader;
		}

		/// <summary>
		/// Reads the ShapeRecord.
		/// </summary>
		/// <param name="Reader">The <see cref="BinaryReader"/> used to read the shapefile.</param>
		public void Read(BinaryReader Reader)
		{
			br = Reader;
			try
			{
				ParseHeader();
				switch (stType)
				{
					case ShapeType.Null:
						br.ReadInt32();
						break;
					case ShapeType.Point:
						lst.Add(ParsePoint(br));
						break;
					case ShapeType.MultiPoint:
						foreach (GoogleMarker m in ParseMultiPoint(br)) { lst.Add(m); }
						break;
					case ShapeType.PolyLine:
						foreach (GooglePolyline l in ParsePolyline(br)) { lst.Add(l); }
						break;
					case ShapeType.Polygon:
						foreach (GooglePolygon p in ParsePolygon(br)) { lst.Add(p); }
						break;
					default:
						Debug.WriteLine("Unknown shape found.");
						if (rus != null)
						{
							MemoryStream ms = new MemoryStream();
							Byte[] b = new byte[intLength];
							br.Read(b, 0, intLength);
							ms.Write(b, 0, b.Length);
							ms.Seek(0, SeekOrigin.Begin);
							rus((int)stType, intLength, ms, ref lst);
						}
						break;
				}
			}
			catch (Exception ex) { strError = ex.Message; }
		}

		#endregion

		#region Properties

		/// <summary>
		/// Gets or sets the error message for the record.
		/// </summary>
		public string Error
		{
			get { return strError; }
			set { strError = value; }
		}

		/// <summary>
		/// Gets the number of the record in the shapefile.
		/// </summary>
		public int RecordNumber
		{
			get { return intRecord; }
		}

		/// <summary>
		/// Gets a <see cref="List{GoogleOverlay}"/> contained in the record.
		/// </summary>
		public List<GoogleOverlay> Overlays
		{
			get { return lst; }
		}

		/// <summary>
		/// Gets the type of shape of the record.
		/// </summary>
		public ShapeType ShapeType
		{
			get { return stType; }
		}

		#endregion

		#region Methods

		#region Private

		private void ParseHeader()
		{
			intRecord = br.ReadInt32();
			intLength = br.ReadInt32();
			stType = (ShapeType)br.ReadInt32();
		}

		private GoogleBounds ParseBoundingBox(System.IO.BinaryReader br)
		{
			GoogleBounds data = new GoogleBounds();
			data.MinLongitude = br.ReadDouble();
			data.MinLatitude = br.ReadDouble();
			data.MaxLongitude = br.ReadDouble();
			data.MaxLatitude = br.ReadDouble();

			return data;
		}

		private int intPoints = 0;

		private GoogleMarker ParsePoint(System.IO.BinaryReader br)
		{
			GoogleMarker data = new GoogleMarker();
			double lat, lng;
			data.ID = Guid.NewGuid().ToString();
			lng = br.ReadDouble();
			lat = br.ReadDouble();
			if (pc != null) { data.Point = pc(lng, lat); }
			else
			{
				data.Point.Longitude = lng;
				data.Point.Latitude = lat;
			}
			//Debug.WriteLine(data.Point.ToUrlString());
			intPoints++;
			return data;
		}

		private List<GoogleMarker> ParseMultiPoint(System.IO.BinaryReader br)
		{
			List<GoogleMarker> data = new List<GoogleMarker>();
			ParseBoundingBox(br);
			data = new List<GoogleMarker>(br.ReadInt32());
			Debug.WriteLine("Multipoint started - capacity: " + data.Capacity.ToString());
			for (int i = 0; i < data.Capacity; i++)
			{
				GoogleMarker ll = ParsePoint(br);
				//Debug.WriteLine(ll.ID + " - " + ll.Point.ToUrlString());
				data.Add(ll);
			}
			Debug.WriteLine("Multipoint ended.");
			return data;
		}

		// Kaveh: I hated these debug.writeline calls so I commnted them out. It was slowing down the debugging and keep messing up the debug window.
		private List<GooglePolyline> ParsePolyline(System.IO.BinaryReader br)
		{
			List<GooglePolyline> data;
			ParseBoundingBox(br);
			List<GoogleLatLng> points = new List<GoogleLatLng>();
			data = new List<GooglePolyline>(br.ReadInt32());
			// Debug.WriteLine("Polyline capacity: " + data.Capacity.ToString());
			int numpoints = br.ReadInt32();
			// Debug.WriteLine("Number of points: " + numpoints.ToString());
			int[] parts = new int[data.Capacity];

			// Debug.WriteLine("Started reading parts indices.");
			for (int i = 0; i < data.Capacity; i++)
			{
				parts[i] = br.ReadInt32();
				// Debug.WriteLine(string.Format("Part {0} index {1}", i.ToString(), parts[i].ToString()));
			}
			// Debug.WriteLine("Finished reading parts indices.");
			// Debug.WriteLine("Started reading points.");
			for (int i = 0; i < numpoints; i++)
			{
				GoogleLatLng ll = ParsePoint(br).Point;
				//Debug.WriteLine(ll.ToUrlString());
				points.Add(ll);
			}
			// Debug.WriteLine("Finished reading points.");
			int lines_read = 0;
			// Debug.WriteLine("Started creating lines.");
			if (parts.Length == 1)
			{
				GooglePolyline line = new GooglePolyline();
				line.ID = Guid.NewGuid().ToString();
				line.Points = points;
				data.Add(line);
				lines_read++;
				// Debug.WriteLine(lines_read.ToString() + " lines created.");
			}
			else
			{
				for (int i = 1; i <= parts.Length; i++)
				{
					GooglePolyline line = new GooglePolyline();
					line.ID = Guid.NewGuid().ToString();
					// Debug.WriteLine(line.ID);
					if (i != parts.Length) line.Points = points.GetRange(parts[i - 1], parts[i] - parts[i - 1]);
					else line.Points = points.GetRange(parts[i - 1], points.Count - parts[i - 1]);
					data.Add(line);
					lines_read++;
					// Debug.WriteLine(lines_read.ToString() + " lines created.");
				}
			}
			// Debug.WriteLine("Finished creating lines.");
			return data;
		}

		private List<GooglePolygon> ParsePolygon(System.IO.BinaryReader br)
		{
			List<GooglePolyline> pl = ParsePolyline(br);
			List<GooglePolygon> pg = new List<GooglePolygon>();
			foreach (GooglePolyline l in pl)
			{
				pg.Add(l);
			}
			pl = null;
			return pg;
		}

		#endregion

        public SqlGeography ToUnionSqlGeography(string Name)
        {
            return ToUnionSqlGeography(Name, 4326);
		}
		
		public SqlGeometry ToUnionSqlGeometry(string Name)
		{
			return ToUnionSqlGeometry(Name, 4326);
		}

		/// <summary>
		/// Added by Kaveh - Returns a SqlGeometry object built from union of all the underlying GoogleOvelays
		/// </summary>
		/// <param name="Name">Name for for the overlay folder.</param>
		/// <param name="SRID">The SRID of the shapefile.</param>
		/// <returns>A <see cref="SqlGeometry"/> value</returns>
		public SqlGeometry ToUnionSqlGeometry(string Name, int SRID)
		{
			SqlGeometry g = null;
			if (lst.Count > 0)
			{
				g = lst[0].ToSqlGeometry(Name, SRID);

				if (g == null)
				{
					g = lst[0].ToSqlGeometry(Name, SRID);
				}

				for (int i = 1; i < lst.Count; i++)
				{
					SqlGeometry nextGeometry = lst[i].ToSqlGeometry(Name, SRID);
					SqlGeometry unionedGeometry = g.STUnion(nextGeometry);
					if (unionedGeometry != null)
					{
						g = unionedGeometry;
					}
				}
			}
			return g;
		}

		/// <summary>
		/// Added by Kaveh - Returns a SqlGeography object built from union of all the underlying GoogleOvelays
		/// </summary>
		/// <param name="Name">Name for for the overlay folder.</param>
        /// <param name="SRID">The SRID of the shapefile.</param>
		/// <returns>A <see cref="SqlGeography"/> value</returns>
		public SqlGeography ToUnionSqlGeography(string Name, int SRID)
		{
			SqlGeography g = null;
			if (lst.Count > 0)
			{
				g = lst[0].ToSqlGeography(Name, SRID);

                for (int i = 1; i < lst.Count; i++)
                {
                    SqlGeography nextGeography = lst[i].ToSqlGeography(Name, SRID);
                    SqlGeography unionedGeography = g.STUnion(nextGeography);
                    if (unionedGeography != null)
                    {
                        g = unionedGeography;
                    }
                    else
                    {
                        string here = "";
                    }
                }
			}
			return g;
		}

		#endregion
	}
}