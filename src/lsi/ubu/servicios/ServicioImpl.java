package lsi.ubu.servicios;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lsi.ubu.excepciones.AlquilerCochesException;
import lsi.ubu.util.PoolDeConexiones;

public class ServicioImpl implements Servicio {
	private static final Logger LOGGER = LoggerFactory.getLogger(ServicioImpl.class);

	private static final int DIAS_DE_ALQUILER = 4;

	public void alquilar(String nifCliente, String matricula, Date fechaIni, Date fechaFin) throws SQLException {
		PoolDeConexiones pool = PoolDeConexiones.getInstance();

		Connection con = null;
		PreparedStatement st = null;
		ResultSet rs = null;

		/*
		 * El calculo de los dias se da hecho
		 */
		long diasDiff = DIAS_DE_ALQUILER;
		if (fechaFin != null) {
			diasDiff = TimeUnit.MILLISECONDS.toDays(fechaFin.getTime() - fechaIni.getTime());

			if (diasDiff < 1) {
				throw new AlquilerCochesException(AlquilerCochesException.SIN_DIAS);
			}
		}

		try {
			con = pool.getConnection();

			/* A completar por el alumnado... */

			/* ================================= AYUDA R�PIDA ===========================*/
			/*
			 * Algunas de las columnas utilizan tipo numeric en SQL, lo que se traduce en
			 * BigDecimal para Java.
			 * 
			 * Convertir un entero en BigDecimal: new BigDecimal(diasDiff)
			 * 
			 * Sumar 2 BigDecimals: usar metodo "add" de la clase BigDecimal
			 * 
			 * Multiplicar 2 BigDecimals: usar metodo "multiply" de la clase BigDecimal
			 *
			 * 
			 * Paso de util.Date a sql.Date java.sql.Date sqlFechaIni = new
			 * java.sql.Date(sqlFechaIni.getTime());
			 *
			 *
			 * Recuerda que hay casos donde la fecha fin es nula, por lo que se debe de
			 * calcular sumando los dias de alquiler (ver variable DIAS_DE_ALQUILER) a la
			 * fecha ini.
			 */
			
			 // 1) Comprobar vehículo
            st = con.prepareStatement("SELECT id_modelo FROM vehiculos WHERE matricula = ?");
            st.setString(1, matricula);
            rs = st.executeQuery();
            Integer idModelo = null;
            if (rs.next()) {
                idModelo = rs.getInt(1);
            } else {
                throw new AlquilerCochesException(AlquilerCochesException.VEHICULO_NO_EXIST);
            }

            rs.close(); 
            st.close();
            
             // 2) Comprobar cliente
            st = con.prepareStatement("SELECT 1 FROM clientes WHERE NIF = ?");
            st.setString(1, nifCliente);
            rs = st.executeQuery();
            if (!rs.next()) {
                throw new AlquilerCochesException(AlquilerCochesException.CLIENTE_NO_EXIST);
            }
            rs.close(); 
            st.close();
            
            	// 2.5) Si no se especifica fechaFin, usar nuevo modelo máximo
            if (fechaFin == null) {
                st = con.prepareStatement("SELECT MAX(id_modelo) FROM modelos");
                rs = st.executeQuery();
                if (rs.next()) {
                    idModelo = rs.getInt(1);
                }
                rs.close(); st.close();
            }

            
            
         // 3) Formatear fechas SQL
            java.sql.Date sqlFechaIni = new java.sql.Date(fechaIni.getTime());
            java.sql.Date sqlFechaFin = (fechaFin != null)
                    ? new java.sql.Date(fechaFin.getTime())
                    : null;
            
         // 4) Comprobar solape se reservas
            st = con.prepareStatement(
                "SELECT COUNT(*) FROM reservas " +
                "WHERE matricula = ? AND fecha_ini <= ? " +
                "AND (fecha_fin IS NULL OR fecha_fin >= ?)"
            );
            st.setString(1, matricula);
            st.setDate(2, (sqlFechaFin != null) ? sqlFechaFin : sqlFechaIni);
            st.setDate(3, sqlFechaIni);
            rs = st.executeQuery(); 
            rs.next();
            if (rs.getInt(1) > 0) {
                throw new AlquilerCochesException(AlquilerCochesException.VEHICULO_OCUPADO);
            }
            rs.close(); 
            st.close();

            // 5) Insertar reservas en tabla reservas
            st = con.prepareStatement(
                "INSERT INTO reservas(idReserva, cliente, matricula, fecha_ini, fecha_fin) " +
                "VALUES(seq_reservas.nextval, ?, ?, ?, ?)"
            );
            st.setString(1, nifCliente);
            st.setString(2, matricula);
            st.setDate(3, sqlFechaIni);
            st.setDate(4, sqlFechaFin);
            st.executeUpdate();
            st.close();

            // 6) Obtener Datos del modelo seleccionado
            st = con.prepareStatement(
                "SELECT precio_cada_dia, capacidad_deposito, tipo_combustible " +
                "FROM modelos WHERE id_modelo = ?"
            );
            st.setInt(1, idModelo);
            rs = st.executeQuery(); 
            rs.next();
            BigDecimal precioDia       = rs.getBigDecimal(1);
            int        capacidadDepo   = rs.getInt(2);
            String     tipoCombustible = rs.getString(3);
            rs.close(); 
            st.close();

            // 7) Datos precio por litro
            st = con.prepareStatement(
                "SELECT precio_por_litro FROM precio_combustible WHERE tipo_combustible = ?"
            );
            st.setString(1, tipoCombustible);
            rs = st.executeQuery(); 
            rs.next();
            BigDecimal precioLitro = rs.getBigDecimal(1);
            rs.close(); 
            st.close();
            
          
            
            
     
		} catch (SQLException e) {
			// Completar por el alumno

			LOGGER.debug(e.getMessage());

			throw e;

		} finally {
			/* A rellenar por el alumnado*/
		}
	}
}
