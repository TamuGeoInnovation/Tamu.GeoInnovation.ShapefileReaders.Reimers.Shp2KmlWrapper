using System;

namespace Reimers.Esri
{
	public class ShapefileRecord
	{
		private object[] dataArray = null;

		public ShapeRecord Shape { get; set; }
		public double SteamedBytesRatio { get; set; }

		public object[] DataArray
		{
			get { return dataArray; }
			set { dataArray = value; }
		}

		public ShapefileRecord(object[] items, ShapeRecord shape)
		{
			Shape = shape;
			dataArray = items;
		}

		public ShapefileRecord()
			: this(null, null)
		{ }
	}
}