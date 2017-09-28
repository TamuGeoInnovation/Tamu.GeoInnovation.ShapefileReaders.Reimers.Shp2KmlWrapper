using System;
using System.Collections.Generic;

namespace Reimers.Esri
{
    /// <summary>
    /// Converts from the French Lambert projection to WGS84.
    /// </summary>
    public class LambertConvert
    {
        private static double ALG0001(double phi, double e)
        {
            double temp = (1 - (e * Math.Sin(phi))) / (1 + (e * Math.Sin(phi)));

            double L = Math.Log(Math.Tan((Math.PI / 4) + (phi / 2)) * Math.Pow(temp, (e / 2)));

            return L;
        }

        private static double ALG0002(double L, double e, double epsilon)
        {
            List<double> phi = new List<double>();
            phi.Add(2 * Math.Atan(Math.Exp(L)) - (Math.PI / 2));

            int i = 0;
            do
            {
                i++;
                double temp = (1 + (e * Math.Sin(phi[i - 1]))) / (1 - (e * Math.Sin(phi[i - 1])));
                phi.Add(2 * Math.Atan(Math.Pow(temp, (e / 2)) * Math.Exp(L)) - Math.PI / 2);
            }
            while (Math.Abs(phi[i] - phi[i - 1]) >= epsilon);

            return phi[i];
        }

        private static double[] ALG0004(double X, double Y, double n, double c, double Xs, double Ys, double lambdac, double e, double epsilon)
        {
            double[] coords = new double[2];
            double R = Math.Sqrt(Math.Pow((X - Xs), 2) + Math.Pow((Y - Ys), 2));
            double gamma = Math.Atan((X - Xs) / (Ys - Y));

            double lambda = lambdac + (gamma / n);

            double L = (-1 / n) * Math.Log(Math.Abs(R / c));

            double phi = ALG0002(L, e, epsilon);

            coords[0] = lambda; //'lambda'
            coords[1] = phi;//'phi'

            return coords;
        }

        private static double[] ALG0009(double lambda, double phi, double he, double a, double e)
        {
            double[] coords = new double[3];
            double N = ALG0021(phi, a, e);

            double X = (N + he) * Math.Cos(phi) * Math.Cos(lambda);

            double Y = (N + he) * Math.Cos(phi) * Math.Sin(lambda);

            double Z = (N * (1 - e * e) + he) * Math.Sin(phi);

            coords[0] = X;//'X'
            coords[1] = Y;//'Y'
            coords[2] = Z;//'Z'

            return coords;
        }

        private static double[] ALG0012(double X, double Y, double Z, double a, double e, double epsilon)
        {
            List<double> phi = new List<double>();
            double[] coords = new double[3];
            double lambda = Math.Atan(Y / X);

            double P = Math.Sqrt(X * X + Y * Y);
            phi.Add(Math.Atan(Z / (P * (1 - ((a * e * e) / Math.Sqrt(X * X + Y * Y + Z * Z))))));

            int i = 0;
            do
            {
                i++;
                double temp = Math.Pow((1 - (a * e * e * Math.Cos(phi[i - 1]) / (P * Math.Sqrt(1 - e * e * Math.Sin(phi[i - 1]) * Math.Sin(phi[i - 1]))))), -1);
                phi.Add(Math.Atan(temp * Z / P));
            }
            while (Math.Abs(phi[i] - phi[i - 1]) >= epsilon);

            double phix = phi[i];

            double he = (P / Math.Cos(phix)) - (a / Math.Sqrt(1 - e * e * Math.Sin(phix) * Math.Sin(phix)));

            coords[0] = lambda;//'lambda'
            coords[1] = phix;//'phi'
            coords[2] = he;//'he'

            return coords;
        }

        private static double[] ALG0013(double Tx, double Ty, double Tz, double D, double Rx, double Ry, double Rz, double[] U)
        {
            double[] V = new double[3];
            V[0] = Tx + U[0] * (1 + D) + U[2] * Ry - U[1] * Rz;
            V[1] = Ty + U[1] * (1 + D) + U[0] * Rz - U[2] * Rx;
            V[2] = Tz + U[2] * (1 + D) + U[1] * Rx - U[0] * Ry;

            return V;
        }

        private static double[] ALG0019(double lambda0, double phi0, double k0, double X0, double Y0, double a, double e)
        {
            double[] tab = new double[6];
            //$lambdac = $lambda0;
            double n = Math.Sin(phi0);
            double C = k0 * ALG0021(phi0, a, e) * Math.Tan(Math.PI / 2 - phi0) * Math.Exp(n * ALG0001(phi0, e));
            //$Xs = $X0;
            Y0 = Y0 + k0 * ALG0021(phi0, a, e) * Math.Tan(Math.PI / 2 - phi0);

            tab[0] = e; //'e'
            tab[1] = n;//'n'
            tab[2] = C;//'C'
            tab[3] = lambda0;//'lambdac'
            tab[4] = X0;//'Xs'
            tab[5] = Y0;//'Ys'

            return tab;
        }

        private static double ALG0021(double phi, double a, double e)
        {
            double N = a / Math.Sqrt(1 - e * e * Math.Sin(phi) * Math.Sin(phi));
            return N;
        }

        private static double[] ALG0054(double lambda0, double phi0, double X0, double Y0, double phi1, double phi2, double a, double e)
        {
            double[] tab = new double[6];
            //$lambdac = $lambda0;
            double n = ((Math.Log((ALG0021(phi2, a, e) * Math.Cos(phi2)) / (ALG0021(phi1, a, e) * Math.Cos(phi1)))) / (ALG0001(phi1, e) - ALG0001(phi2, e)));
            double C = ((ALG0021(phi1, a, e) * Math.Cos(phi1)) / n) * Math.Exp(n * ALG0001(phi1, e));

            if (phi0 != (Math.PI / 2))
                //{
                //    Xs = X0;
                //    Ys = Y0;
                //}
                //else
                //{			
                //Xs = X0;
                Y0 = Y0 + C * Math.Exp(-1 * n * ALG0001(phi0, e));
            //}

            tab[0] = e;//'e'
            tab[1] = n;//'n'
            tab[2] = C;//'C'
            tab[3] = lambda0;//'lambdac'
            tab[4] = X0;//'Xs'
            tab[5] = Y0;//'Ys'

            return tab;
        }

        /// <summary>
        /// Performs a conversion from the specified Lambert projection to WGS84.
        /// </summary>
        /// <param name="Spec">The projection specification.</param>
        /// <param name="X">The X value of the coordinate.</param>
        /// <param name="Y">The Y value of the coordinate.</param>
        /// <returns>A <see cref="Reimers.Map.GoogleLatLng"/> object.</returns>
        public static Reimers.Map.GoogleLatLng Lambert2WGS84(string Spec, double X, double Y)//$orig
        {
            double epsilon = 0.00000000001;
            double n, c, Xs, Ys, lambdac, e, he, a, Tx, Ty, Tz, D, Rx, Ry, Rz = 0;

            switch (Spec)
            {
                case "L93":
                    n = 0.7256077650;
                    c = 11745255.426;
                    Xs = 700000;
                    Ys = 12655612.050;
                    lambdac = 0.04079234433;
                    e = 0.08248325676;
                    he = 100;
                    a = 6378249.2;
                    Tx = -168;
                    Ty = -60;
                    Tz = +320;
                    D = 0;
                    Rx = 0;
                    Ry = 0;
                    Rz = 0;
                    break;
                default:
                    n = 0.7289686274;
                    c = 11745793.39;
                    Xs = 600000;
                    Ys = 6199695.768;
                    lambdac = 0.04079234433;
                    e = 0.08248325676;
                    he = 100;
                    a = 6378249.2;
                    Tx = -168;
                    Ty = -60;
                    Tz = +320;
                    D = 0;
                    Rx = 0;
                    Ry = 0;
                    Rz = 0;
                    break;               
            }

            double[] coords = ALG0004(X, Y, n, c, Xs, Ys, lambdac, e, epsilon);
            coords = ALG0009(coords[0], coords[1], he, a, e);
            coords = ALG0013(Tx, Ty, Tz, D, Rx, Ry, Rz, coords);
            a = 6378137.0;
            double f = 1 / 298.257223563;
            double b = a * (1 - f);
            e = Math.Sqrt((a * a - b * b) / (a * a));
            X = coords[0];
            Y = coords[1];
            double Z = coords[2];
            coords = ALG0012(X, Y, Z, a, e, epsilon);
            Reimers.Map.GoogleLatLng xy = new Reimers.Map.GoogleLatLng();
            xy.Longitude = (coords[0] / Math.PI) * 180.0;
            xy.Latitude = (coords[1] / Math.PI) * 180.0;
            return xy;
        }
    }
}
