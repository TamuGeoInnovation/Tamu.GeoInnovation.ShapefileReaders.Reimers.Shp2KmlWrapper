using System.IO;
using Reimers.Map;
using System.Collections.Generic;
namespace Reimers.Esri
{
	/// <summary>
	/// Handles reading of unknown shapefile data.
	/// </summary>
	/// <param name="Type">The <see cref="ShapeType"/> of the <see cref="ShapeRecord"/>.</param>
	/// <param name="Length">The size of the <see cref="ShapeRecord"/> in <see cref="byte"/> as <see cref="int"/>.</param>
	/// <param name="Data">The <see cref="ShapeRecord"/> data in a <see cref="Stream"/>.</param>
	/// <param name="Overlays">The <see cref="GoogleOverlay"/> contained in the <see cref="ShapeRecord"/>.</param>
	/// <returns>A <see cref="ShapeRecord"/> object.</returns>
	public delegate ShapeRecord ReadUnkownShape(int Type, int Length, Stream Data, ref List<GoogleOverlay> Overlays);

	/// <summary>
	/// Default handler for <see cref="Shapefile"/> events.
	/// </summary>
	public delegate void ShapefileHandler();

	/// <summary>
	/// Handles writing of custom overlay types.
	/// </summary>
	/// <param name="Overlay">The <see cref="GoogleOverlay"/> to write.</param>
	/// <param name="MetaData">The shapefile metadata as an object array.</param>
	/// <returns>A KML representation of the <see cref="GoogleOverlay"/> as a <see cref="string"/>.</returns>
	public delegate string OverlayWriter(GoogleOverlay Overlay, object[] MetaData);

	/// <summary>
	/// Handles point data after parsing.
	/// </summary>
	/// <param name="X">The horizontal (longitudal) value parsed from the shapefile data.</param>
	/// <param name="Y">The vertical (latitudal) value parsed from the shapefile data.</param>
	/// <returns>A <see cref="GoogleLatLng"/> object.</returns>
	public delegate GoogleLatLng PointParsedHandler(double X, double Y);

    /// <summary>
    /// Handles a shapefile record being read.
    /// </summary>
    /// <param name="numberOfRecordsRead">The number of records that have been read from the shapefile.</param>
    /// <returns>void</returns>
    public delegate void ShapefileRecordReadHandler(int numberOfRecordsRead);

    /// <summary>
    /// Handles a dbf record being read.
    /// </summary>
    /// <param name="numberOfRecordsRead">The number of records that have been read from the dbf.</param>
    /// <returns>void</returns>
    public delegate void DbfRecordReadHandler(int numberOfRecordsRead);

    /// <summary>
    /// Handles a percent of the stream being read.
    /// </summary>
    /// <param name="numberOfRecordsRead">The percent of records that have been read from the stream.</param>
    /// <returns>void</returns>
    public delegate void PercentReadHandler(double percentRead);

    /// <summary>
    /// Handles a number of records being read from the stream.
    /// </summary>
    /// <param name="numberOfRecordsRead">The number of records that have been read from the stream.</param>
    /// <param name="totalNumberOfRecordsRead">The total number of records in the shapefile.</param>
    /// <returns>void</returns>
    public delegate void RecordsReadHandler(int recordsRead, int totalRecords);

    /// <summary>
    /// Handles the number of dbf records being read.
    /// </summary>
    /// <param name="numberOfRecordsRead">The total number of records in the dbf.</param>
    /// <returns>void</returns>
    public delegate void DbfNumberOfRecordsReadHandler(int numberOfRecords);


    /// <summary>
    /// Handles a record having a value computed.
    /// </summary>
    /// <param name="numberOfRecordsComputed">The number of records that have had values computed.</param>
    /// <returns>void</returns>
    public delegate void ShapefileRecordConvertedHandler(int numberOfRecordsComputed);
}