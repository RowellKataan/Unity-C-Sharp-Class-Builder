// ===========================================================================================================
//
// Class/Library: DAL - Data Access Library Class
//        Author: Michael Marzilli   ( http://www.linkedin.com/in/michaelmarzilli )
//       Created: Aug 15, 2007
//	
// VERS 1.0.000 : Aug 15, 2007 : Original File Created.
//      1.1.001 : Mar 26, 2016 : Released for Unity 3D.
//			1.1.002 : Apr 16, 2016 : Resolved bug where "localhost" was being translated to an IPv6 address.
//			1.2.003 : Sep 27, 2017 : Added functionality to allow the class to read from SQLite Databases (direct SQL queries).
//															 Added functionality to allow the class to read from MySQL  Databases (direct SQL queries & stored procedures).
//
// ===========================================================================================================

// COMPILER DIRECTIVES
#define	USES_UNITY

#if USES_UNITY
	#if UNITY_EDITOR
		#define		IS_DEBUGGING
	#else 
		#undef		IS_DEBUGGING
	#endif

	#undef	USES_APPLICATIONMANAGER		// #define = Scene has an ApplicationManager Prefab,	#undef = Scene does not have an ApplicationManager Prefab
	#undef	USES_STATUSMANAGER				// #define = Scene has a  StatusManager Prefab,				#undef = Scene does not have a  StatusManager Prefab
#endif


// REFERENCE DECLARATIONS
using System;
using System.Collections;
using System.Data;
using System.Data.SqlClient;
using Mono.Data.Sqlite;
using MySql.Data;
using MySql.Data.MySqlClient;

public class ClsDAL
{

	#region "PRIVATE CONSTANTS"

		private int			CONNECTION_TIMEOUT		= 8;
		private int			QUERY_TIMEOUT					= 600;

	#endregion

	#region "PRIVATE VARIABLES"

		private DBtypes										_dbType	= DBtypes.MSSQL;		// USE MICROSOFT SQL (MSSQL) AS DEFAULT.  CAN OVERRIDE WHEN THE CLASS INSTANCE IS CREATE.


		// MS-SQL CONNECTION VARIABLES
		private SqlConnection							_sqlConn;
		private SqlCommand								_sqlComm;
		private SqlParameterCollection		_SQLSPparams;

		// MYSQL  CONNECTION VARIABLES
		private MySqlConnection						_mySqlConn;
		private MySqlCommand							_mySqlComm;
		private MySqlParameterCollection	_mySqlParams;

		// SQLITE CONNECTION VARIABLES
		private SqliteConnection					_sqliteConn;
		private SqliteCommand							_sqliteComm;

		// SQL SERVER CONNECTION STATES
		private bool				_blnIsOnline						= false;
		private bool				_blnIsConnecting				= false;
		private bool				_blnIsProcessing				= false;
		private bool				_blnIsDisposed					= false;
		private bool				_blnIsFailed						= false;

		// SQL SERVER/DATABASE INFORMATION
		private bool				_blnUseWindowsAccount		= false;
		private bool				_blnKeepConnectionOpen	= false;
		private	string			_strDBserver;
		private	string			_strDBdatabase;
		private	string			_strDBuser;
		private	string			_strDBpassword;
		private int					_intDBport;
		private string			_strServerIPaddress			= "";

		private string			_strSQLiteDBlocation		= "";

		// SQL REPORTING
		private string		  _strErrors							= "";
		private string		  _strSQLqueries					= "";
		private Util.Timer	_queryTimer							= null;
		private int					_intQueryCount					= 0;
		private float				_fQueryAverage					= 0;
		private float				_fQueryLast							= 0;

	#endregion

	#region "PUBLIC PROPERTIES"

		public	enum		DBtypes : int { MSSQL = 0, MYSQL = 1, SQLITE = 2 }
		public	DBtypes	DBtype
		{
			get
			{
				return _dbType;
			}
			set
			{
				_dbType = value;
			}
		}

		public	string	DBserver
		{
			get
			{
				return _strDBserver;
			}
			set
			{
				_strDBserver = value.Trim();
			}
		}
		public	int			DBport
		{
			get
			{
				return _intDBport;
			}
			set
			{
				_intDBport = value;
				if (_intDBport < 0)
						_intDBport = 0;
			}
		}
		public	string	DBdatabase
		{
			get
			{
				return _strDBdatabase;
			}
			set
			{
				_strDBdatabase = value.Trim();
			}
		}
		public	string	DBuser
		{
			get
			{
				return _strDBuser;
			}
			set
			{
				_strDBuser = value.Trim();
			}
		}
		public	string	DBpassword
		{
			get
			{
				return _strDBpassword;
			}
			set
			{
				_strDBpassword = value.Trim();
			}
		}
		public	bool		UseWindowsAccount
		{
			get
			{
				return _blnUseWindowsAccount;
			}
			set
			{
				_blnUseWindowsAccount = value;
			}
		}
		public	bool		KeepConnectionOpen
		{
			get
			{
				return _blnKeepConnectionOpen;
			}
			set
			{
				_blnKeepConnectionOpen = value;
			}
		}

		public	string	DBserverString
		{
			get
			{
				// GET THE IPv4 OF THE LOCALHOST (IF APPLICABLE)
				// THIS CONVERTS THE SERVER NAME INTO AN IP ADDRESS
				// THIS IS DONE TO AVOID THE SERVER NAME BEING CONVERTED TO AN IPv6 ADDRESS
				if (_dbType == DBtypes.MSSQL && _strDBserver.ToLower().Contains("localhost"))
				{ 
					if (_strServerIPaddress == "")
					{
						string strTempServer	= "";
						string strTempDB			= "";
						if (_strDBserver.Contains("\\"))
						{
							strTempServer = _strDBserver.Split('\\')[0];
							strTempDB			= _strDBserver.Split('\\')[1];
						} else
							strTempServer = _strDBserver;

						System.Net.NetworkInformation.NetworkInterface[] networkInterfaces = System.Net.NetworkInformation.NetworkInterface.GetAllNetworkInterfaces();
						foreach (System.Net.NetworkInformation.NetworkInterface network in networkInterfaces)
						{
							// Read the IP configuration for each network 
							System.Net.NetworkInformation.IPInterfaceProperties properties = network.GetIPProperties();

							// Each network interface may have multiple IP addresses 
							foreach (System.Net.NetworkInformation.IPAddressInformation address in properties.UnicastAddresses)
							{
								// We're only interested in IPv4 addresses for now 
								if (address.Address.AddressFamily != System.Net.Sockets.AddressFamily.InterNetwork)
									continue;

								// Ignore loopback addresses (e.g., 127.0.0.1) 
								if (System.Net.IPAddress.IsLoopback(address.Address))
								{
									_strServerIPaddress = address.Address.ToString();
									if (strTempDB != "")
										_strServerIPaddress += "\\" + strTempDB;
									break;
								}
							}
						} 

						if (_strServerIPaddress == "")
						{ 
							System.Net.IPAddress[] ips = System.Net.Dns.GetHostAddresses(strTempServer);
							foreach (System.Net.IPAddress a in ips)
							{
								if (a.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork)
								{
									_strServerIPaddress = a.ToString();
									if (strTempDB != "")
										_strServerIPaddress += "\\" + strTempDB;
									break;
								}
							}
						}
					}
				} else 
					_strServerIPaddress = _strDBserver;

				// ADD ON THE PORT (IF APPLICABLE)
				if (_intDBport == 0 || (_dbType == DBtypes.MSSQL && _intDBport == 1433) || (_dbType == DBtypes.MYSQL && _intDBport == 3306))
					return _strServerIPaddress;
				else
					return _strServerIPaddress + "," + _intDBport.ToString();
			}
		}
		public	bool		IsOnline
		{
			get
			{
				return _blnIsOnline;
			}
		}
		public	bool		IsConnectionFailed
		{
			get
			{
				return _blnIsFailed;
			}
		}
		public	bool		IsProcessing
		{
			get
			{
				return _blnIsProcessing;
			}
		}

		public	string	SQLiteDBfileLocation
		{
			get
			{
				return _strSQLiteDBlocation;
			}
			set
			{
				_strSQLiteDBlocation = value.Trim();
			}
		}

	#endregion


	#region "MS-SQL CODE"

		#region "CONNECTION FUNCTIONS"
					
					private string				SQLConnectionString
					{
						get
						{
							if (UseWindowsAccount)
//							return "Server=" + DBserverString + ";Database=" + DBdatabase + ";Integrated Security=True;Trusted_Connection=True;";
								return "Server=" + DBserverString + ";Database=" + DBdatabase + ";Integrated Security=SSPI;";		//Trusted_Connection=Yes;
							else
								return "Server=" + DBserverString + ";Database=" + DBdatabase + ";User ID=" + DBuser + ";Password=" + DBpassword + ";";
						}
					}

					private void					SQLOpenConnection()
					{
						if (_blnIsConnecting)
								return;

						try
						{
							StartQueryTimer();
							Job.make(SQLOpenConnectionEnum(), true);
						} catch {
							StopQueryTimer();
							_blnIsConnecting	= false;
							_blnIsOnline			= false;
							_blnIsFailed			= true;
						}
					}
					private IEnumerator		SQLOpenConnectionEnum()
					{
						_blnIsOnline			= false;
						_blnIsFailed			= false;
						_blnIsConnecting	= true;

						if (_sqlConn == null) 
						{
							_sqlConn = new SqlConnection(SQLConnectionString);
							_sqlConn.Open();
						} else {
							switch (_sqlConn.State) 
							{
								case ConnectionState.Open:
									_blnIsOnline = true;
									break;
								case ConnectionState.Closed:
									_sqlConn.Dispose();
									_sqlConn = new SqlConnection(SQLConnectionString);
									try 
									{  
										_sqlConn.Open(); 
									} catch (System.Exception ex) { 
										_blnIsFailed = true;
										ReportError("ClsDAL", "SQLOpenConnectionEnum", "The Database does not Exist", ex);
									}
									break;
								case ConnectionState.Broken:
									_sqlConn.Close();
									_sqlConn.Open();
									break;
							}
						}
						Util.Timer clock = new Util.Timer();
						clock.StartTimer();
						while (!_blnIsOnline && clock.GetTime < CONNECTION_TIMEOUT)
						{
							yield return null;
							_blnIsOnline = (_sqlConn.State != ConnectionState.Broken && _sqlConn.State != ConnectionState.Closed);
						}
						clock.StopTimer();
						_sqlConn.StateChange += OnSQLstateChanged;
						_blnIsConnecting	= false;
						_blnIsFailed			= !_blnIsOnline;
						#if USES_STATUSMANAGER
						if (StatusManager.Instance != null)
								StatusManager.Instance.UpdateStatus();
						#endif
					}
					private void					SQLOpenConnection(string strServer, string strDB, int intPort = 0)
					{
						DBserver		= strServer;
						DBdatabase	= strDB;
						DBuser			= "";
						DBpassword	= "";
						DBport			= intPort;
						UseWindowsAccount = true;
						SQLOpenConnection();
					}
					private void					SQLOpenConnection(string strServer, string strDB, string strUser, string strPwd, int intPort = 0)
					{
						DBserver		= strServer;
						DBdatabase	= strDB;
						DBuser			= strUser;
						DBpassword	= strPwd;
						DBport			= intPort;
						UseWindowsAccount = false;
						SQLOpenConnection();
					}

					private void					SQLCloseConnection()
					{
						StopQueryTimer();
						_blnIsConnecting	= false;
						_blnIsOnline			= false;
						_blnIsProcessing	= false;

						if (_sqlConn == null || !_blnIsOnline)
							return;
						else
						{
							if (_sqlComm != null)
									_sqlComm.Dispose();
							_sqlConn.Close();
							_sqlConn.Dispose();
						}
						#if USES_STATUSMANAGER
						if (StatusManager.Instance != null)
								StatusManager.Instance.UpdateStatus();
						#endif
					}
					private bool					SQLisConnected
					{
						get
						{
							if (!_blnIsConnecting && (_sqlConn == null || _sqlConn.State == ConnectionState.Broken || _sqlConn.State == ConnectionState.Closed))
							{ 
								if (_sqlConn == null)
								{
									_sqlConn = new SqlConnection(SQLConnectionString);
									_sqlConn.Open();
								} else if (_sqlConn.State == ConnectionState.Broken || _sqlConn.State == ConnectionState.Closed) {
									_sqlConn.Close();
									_sqlConn.Dispose();
									_sqlConn = new SqlConnection(SQLConnectionString);
									_sqlConn.Open();
								}
							}
							_blnIsOnline = (_sqlConn != null && _sqlConn.State != ConnectionState.Broken && _sqlConn.State != ConnectionState.Closed && !_blnIsConnecting);
							if (_blnIsOnline)
								StartQueryTimer();
							#if USES_STATUSMANAGER
							if (StatusManager.Instance != null)
								StatusManager.Instance.UpdateStatus();
							#endif
							return _blnIsOnline;
						}
					}
					private bool					SQLisConnecting
					{
						get
						{
							return  _blnIsConnecting;
						}
					}

					private void					OnSQLstateChanged(object conn, System.Data.StateChangeEventArgs e)
					{
						_blnIsOnline = (_sqlConn != null && _sqlConn.State != ConnectionState.Broken && _sqlConn.State != ConnectionState.Closed && !_blnIsConnecting);
						#if USES_STATUSMANAGER
						if (StatusManager.Instance != null)
								StatusManager.Instance.UpdateStatus();
						#endif
					}

		#endregion

		#region "PARAMETER DEFINITION FUNCTIONS"

					private void RemoveDuplicateParameter(string strParamName)
					{
						strParamName = strParamName.ToLower();
						for (int i = _SQLSPparams.Count - 1; i >= 0; i--)
						{
							if (_SQLSPparams[i].ParameterName.ToLower() == strParamName)
							{
								_SQLSPparams.RemoveAt(i);
								break;
							}
						}
					}

					private void SQLAddParam(string strParamName, string    strParamValue)
					{
						if (!SQLisConnected)
							return;
							 
						try
						{
							RemoveDuplicateParameter(strParamName);
							SqlParameter sParam		= new SqlParameter();
							sParam.ParameterName	= strParamName;
							sParam.SqlDbType			= SqlDbType.VarChar;
							sParam.SqlValue				= strParamValue;
							sParam.Direction			= ParameterDirection.Input;
							_SQLSPparams.Add(sParam);
						} catch (Exception ex) { ReportError("ClsDAL.cs", "SQLAddParam(STRING)", strParamValue, ex); }
					}
					private void SQLAddParam(string strParamName, int       intParamValue)
					{
						if (!SQLisConnected)
							return;

						try
						{
							RemoveDuplicateParameter(strParamName);
							SqlParameter sParam		= new SqlParameter();
							sParam.ParameterName	= strParamName;
							sParam.SqlDbType			= SqlDbType.Int;
							sParam.SqlValue				= intParamValue;
							sParam.Direction			= ParameterDirection.Input;
							_SQLSPparams.Add(sParam);
						} catch (Exception ex) { ReportError("ClsDAL.cs", "SQLAddParam(INT)", intParamValue.ToString(), ex); }
					}
					private void SQLAddParam(string strParamName, decimal   decParamValue)
					{
						if (!SQLisConnected)
							return;

						try 
						{
							RemoveDuplicateParameter(strParamName);
							SqlParameter sParam  = new SqlParameter();
							sParam.ParameterName = strParamName;
							sParam.SqlDbType     = SqlDbType.Decimal;
							sParam.SqlValue      = decParamValue;
							sParam.Direction     = ParameterDirection.Input;
							_SQLSPparams.Add(sParam);
						} catch (Exception ex) { ReportError("ClsDAL.cs", "SQLAddParam(DECIMAL)", decParamValue.ToString(), ex); }
					}
					private void SQLAddParam(string strParamName, float     fParamValue)
					{
						if (!SQLisConnected)
							return;

						try 
						{
							RemoveDuplicateParameter(strParamName);
							SqlParameter sParam  = new SqlParameter();
							sParam.ParameterName = strParamName;
							sParam.SqlDbType     = SqlDbType.Float;
							sParam.SqlValue      = fParamValue;
							sParam.Direction     = ParameterDirection.Input;
							_SQLSPparams.Add(sParam);
						} catch (Exception ex) { ReportError("ClsDAL.cs", "SQLAddParam(FLOAT)", fParamValue.ToString(), ex); }
					}
					private void SQLAddParam(string strParamName, double    decParamValue)
					{
						if (!SQLisConnected)
							return;

						try
						{
							RemoveDuplicateParameter(strParamName);
							SqlParameter  sParam = new SqlParameter();
							sParam.ParameterName = strParamName;
							sParam.SqlDbType     = SqlDbType.Decimal;
							sParam.SqlValue      = decParamValue;
							sParam.Direction     = ParameterDirection.Input;
							_SQLSPparams.Add(sParam);
						} catch (Exception ex) { ReportError("ClsDAL.cs", "SQLAddParam(DOUBLE)", decParamValue.ToString(), ex); }
					}
					private void SQLAddParam(string strParamName, long      lngParamValue)
					{
						if (!SQLisConnected)
							return;

						try
						{
							RemoveDuplicateParameter(strParamName);
							SqlParameter sParam  = new SqlParameter();
							sParam.ParameterName = strParamName;
							sParam.SqlDbType     = SqlDbType.Float;
							sParam.SqlValue      = lngParamValue;
							sParam.Direction     = ParameterDirection.Input;
							_SQLSPparams.Add(sParam);
						} catch (Exception ex) { ReportError("ClsDAL.cs", "SQLAddParam(LONG)", lngParamValue.ToString(), ex); }
					}
					private void SQLAddParam(string strParamName, DateTime  dateParamValue)
					{
						if (!SQLisConnected)
							return;

						try
						{
							RemoveDuplicateParameter(strParamName);
							SqlParameter sParam = new SqlParameter();
							sParam.ParameterName = strParamName;
							sParam.SqlDbType = SqlDbType.DateTime;
							sParam.SqlValue = dateParamValue;
							sParam.Direction = ParameterDirection.Input;
							_SQLSPparams.Add(sParam);
						} catch (Exception ex) { ReportError("ClsDAL.cs", "SQLAddParam(DATE)", dateParamValue.ToString(), ex); }
					}
					private void SQLAddParam(string strParamName, bool      blnParamValue)
					{
						if (!SQLisConnected)
							return;

						try
						{
							RemoveDuplicateParameter(strParamName);
							SqlParameter sParam  = new SqlParameter();
							sParam.ParameterName = strParamName;
							sParam.SqlDbType     = SqlDbType.Bit;
							sParam.SqlValue      = blnParamValue;
							sParam.Direction     = ParameterDirection.Input;
							_SQLSPparams.Add(sParam);
						} catch (Exception ex) { ReportError("ClsDAL.cs", "SQLAddParam(BOOLEAN)", blnParamValue.ToString(), ex); }
					}
					private void SQLAddParam(string strParamName, byte[]		buffer)
					{
						if (!SQLisConnected)
							return;

						try
						{
							RemoveDuplicateParameter(strParamName);
							SqlParameter sParam		= new SqlParameter();
							sParam.ParameterName	= strParamName;
							sParam.SqlDbType			= SqlDbType.VarBinary;
							sParam.SqlValue				= buffer;
							sParam.Direction			= ParameterDirection.Input;
							_SQLSPparams.Add(sParam);
						} catch (Exception ex) { ReportError("ClsDAL.cs", "SQLAddParam(BYTE[])", "...", ex); }
					}
					private void SQLAddParam(string strParamName, SqlDbType sType)
					{
						if (!SQLisConnected)
							return;

						try
						{
							RemoveDuplicateParameter(strParamName);
							SqlParameter sParam		= new SqlParameter();
							sParam.ParameterName	= strParamName;
							sParam.SqlDbType			= sType;
							sParam.Value					= null;
							sParam.Direction			= ParameterDirection.Output;
							_SQLSPparams.Add(sParam);
						} catch (Exception ex) { ReportError("ClsDAL.cs", "SQLAddParam(OUTPUT)", "", ex); }
					}
					private void SQLAddParam(string strParamName, SqlDbType sType, int varSize)
					{
						if (!SQLisConnected)
							return;

						try
						{
							RemoveDuplicateParameter(strParamName);
							SqlParameter sParam		= new SqlParameter();
							sParam.ParameterName	= strParamName;
							sParam.SqlDbType			= sType;
							sParam.Size						= varSize;
							sParam.Value					= null;
							sParam.Direction			= ParameterDirection.Output;
							_SQLSPparams.Add(sParam);
						} catch (Exception ex) { ReportError("ClsDAL.cs", "SQLAddParam(OUTPUT,SIZE)", "", ex); }
					}
					private void SQLClearParams()
					{
						if (!SQLisConnected && !_blnIsConnecting)
						{
							SQLOpenConnection();
							if (!SQLisConnected)
							{
								_strErrors += "Connection to Database Lost.\n"; 
								return;
							}
						}

						try
						{
							if (_SQLSPparams != null)
								_SQLSPparams.Clear();
						} catch (Exception ex) { ReportError("ClsDAL.cs", "SQLClearParams", "", ex); }
						_sqlComm = new SqlCommand();
						_SQLSPparams = _sqlComm.Parameters;
						try
						{
							if (_SQLSPparams != null)
								_SQLSPparams.Clear();
						} catch (Exception ex) { ReportError("ClsDAL.cs", "SQLClearParams", "", ex); }
					}
					private SqlParameter SQLCopyParameter(SqlParameter aPa)
					{
						SqlParameter	olt = new SqlParameter();
						olt.DbType				= aPa.DbType;
						olt.Direction			= aPa.Direction;
						olt.DbType				= aPa.DbType;
						olt.ParameterName = aPa.ParameterName;
						olt.Size					= aPa.Size;
						olt.Value					= aPa.Value;
						return olt;
					}

		#endregion

		#region "DIRECT SQL SELECT FUNCTIONS"

			private DataTable SQLGetSQLSelectDataTable(	string strSQL)
			{
				DataTable dtRet = new DataTable();

				if (!SQLisConnected)
					return null;
						
				try
				{
					SqlDataAdapter dtaDataAdapter = new SqlDataAdapter(strSQL, _sqlConn);
					dtaDataAdapter.Fill(dtRet);
					dtaDataAdapter.Dispose();
				} catch (Exception ex) { ReportError("ClsDAL.cs", "SQLGetSQLSelectDataTable", strSQL, ex); dtRet = null; }

				if (KeepConnectionOpen)
					StopQueryTimer();
				else
					SQLCloseConnection();

				return dtRet;
			}
			private string    SQLGetSQLSelectString(		string strSQL)
			{
				string strRet = "";
				_sqlComm = null;

				if (!SQLisConnected)
					return strRet;

				try
				{
					_sqlComm = null;
					while (_sqlComm == null)
					{
						_sqlComm = new SqlCommand(strSQL, _sqlConn);
					}
					_sqlComm.CommandTimeout = QUERY_TIMEOUT;
					try 
					{
						strRet = _sqlComm.ExecuteScalar().ToString(); 
					} catch (Exception ex) { ReportError("ClsDAL.cs", "SQLGetSQLSelectString", "Unable to Extract Scalar", ex); strRet = ""; }
				} catch (Exception ex) {
					ReportError("ClsDAL.cs", "SQLGetSQLSelectString", strSQL, ex);
					strRet = "";
				}

				if (KeepConnectionOpen)
					StopQueryTimer();
				else
					SQLCloseConnection();

				return strRet;
			}
			private int       SQLGetSQLSelectInt(				string strSQL)
			{
				int intRet = 0;

				if (!SQLisConnected)
					return intRet;

				string strTemp = SQLGetSQLSelectString(strSQL);
				try
				{
					intRet = int.Parse(strTemp);
				} catch {
					intRet = 0;
				}

				return intRet;
			}
			private decimal   SQLGetSQLSelectDecimal(		string strSQL)
			{
				decimal decRet = 0;

				if (!SQLisConnected)
					return decRet;

				string strTemp = SQLGetSQLSelectString(strSQL);
				try
				{
					decRet = decimal.Parse(strTemp);
				} catch {
					decRet = 0;
				}

				return decRet;
			}
			private float			SQLGetSQLSelectFloat(			string strSQL)
			{
				float fRet = 0;

				if (!SQLisConnected)
					return fRet;

				string strTemp = SQLGetSQLSelectString(strSQL);
				try
				{
					fRet = float.Parse(strTemp);
				} catch {
					fRet = 0;
				}

				return fRet;
			}
			private double    SQLGetSQLSelectDouble(		string strSQL)
			{
				double dblRet = 0;

				if (!SQLisConnected)
					return dblRet;

				string strTemp = SQLGetSQLSelectString(strSQL);
				try
				{
					dblRet = double.Parse(strTemp);
				} catch {
					dblRet = 0;
				}

				return dblRet;
			}
			private bool      SQLDoSQLUpdateDelete(			string strSQL)
			{
				bool blnRet = false;

				if (!SQLisConnected)
					return blnRet;

				_sqlComm = _sqlConn.CreateCommand();
				_sqlComm.CommandText		= strSQL;
				_sqlComm.CommandTimeout = QUERY_TIMEOUT;

				try
				{
					blnRet = (_sqlComm.ExecuteNonQuery() > 0);
				} catch (Exception ex) {
					ReportError("ClsDAL.cs", "SQLDoSQLUpdateDelete", strSQL, ex);
					blnRet = false; 
				}

				if (KeepConnectionOpen)
					StopQueryTimer();
				else
					SQLCloseConnection();

				return blnRet;
			}

		#endregion

		#region "STORED PROCEDURE SELECT FUNCTIONS"

			private DataTable SQLGetSPDataTable(string strSP)
			{
				DataTable dtRet = new DataTable();

				if (!SQLisConnected)
					return null;

				try
				{
					SqlCommand command = new SqlCommand(strSP, _sqlConn);
					using (_sqlConn)
					{
						command.CommandType = CommandType.StoredProcedure;
						command.CommandText = strSP;
						command.CommandTimeout	= QUERY_TIMEOUT;

						foreach (SqlParameter aPa in _SQLSPparams)
						{
							command.Parameters.Add(SQLCopyParameter(aPa));
						}
						SqlDataAdapter dtaDataAdapter = new SqlDataAdapter(command);
						dtaDataAdapter.Fill(dtRet);
						dtaDataAdapter.Dispose();
					}
				} catch (Exception ex) { 
					ReportError("ClsDAL.cs", "SQLGetSPDataTable", strSP, ex); dtRet = null; 
				}

				if (KeepConnectionOpen)
					StopQueryTimer();
				else
					SQLCloseConnection();

				return dtRet;
			}
			private string    SQLGetSPString(string strSP)
			{
				string strRet = "";

				if (!SQLisConnected)
					return strRet;

				try
				{
					_sqlComm = new SqlCommand(strSP, _sqlConn);
					using (_sqlConn)
					{
						string strParamName = "";
						_sqlComm.CommandType = CommandType.StoredProcedure;
						_sqlComm.CommandText = strSP;
						_sqlComm.CommandTimeout	= QUERY_TIMEOUT;

						foreach (SqlParameter sqlpar in _SQLSPparams)
						{
							_sqlComm.Parameters.Add(SQLCopyParameter(sqlpar));
							if (sqlpar.Direction == ParameterDirection.Output)
								strParamName = sqlpar.ParameterName.ToString();
						}
						if (strParamName != "")
						{
							_sqlComm.ExecuteNonQuery();
							strRet = _sqlComm.Parameters[strParamName].Value.ToString();
						} else {
							DataTable dtRet = new DataTable();
							SqlDataAdapter dtaDataAdapter = new SqlDataAdapter(_sqlComm);
							dtaDataAdapter.Fill(dtRet);
							dtaDataAdapter.Dispose();
							try { strRet = dtRet.Rows[0][0].ToString(); } catch { strRet = ""; }
						}
					}
				} catch (Exception ex) { ReportError("ClsDAL.cs", "SQLGetSPString", strSP, ex); }

				if (KeepConnectionOpen)
					StopQueryTimer();
				else
					SQLCloseConnection();

				return strRet;
			}
			private int       SQLGetSPInt(string strSP)
			{
				int intRet = 0;

				if (!SQLisConnected)
					return intRet;

				string strTemp = SQLGetSPString(strSP);
				try
				{
					intRet = int.Parse(strTemp);
				} catch {
					intRet = 0;
				}

				return intRet;
			}
			private long      SQLGetSPLong(string strSP)
			{
				long lngRet = 0;

				if (!SQLisConnected)
					return lngRet;

				string strTemp = SQLGetSPString(strSP);
				try
				{
					lngRet = long.Parse(strTemp);
				} catch {
					lngRet = 0;
				}

				return lngRet;
			}
			private decimal   SQLGetSPDecimal(string strSP)
			{
				decimal decRet = 0;

				if (!SQLisConnected)
					return decRet;

				string strTemp = SQLGetSPString(strSP);
				try
				{
					decRet = decimal.Parse(strTemp);
				} catch {
					decRet = 0;
				}

				return decRet;
			}
			private float		  SQLGetSPFloat(string strSP)
			{
				float		fRet = 0;

				if (!SQLisConnected)
					return fRet;

				string strTemp = SQLGetSPString(strSP);
				try
				{
					fRet = float.Parse(strTemp);
				} catch {
					fRet = 0;
				}

				return fRet;
			}
			private byte[]    SQLGetSPBinary(string strSP)
			{
				byte[]    binRet       = null;
				DataTable dtRet        = new DataTable();

				if (!SQLisConnected)
					return binRet;

				try
				{
					_sqlComm = new SqlCommand(strSP, _sqlConn);
					using (_sqlConn)
					{
						_sqlComm.CommandType = CommandType.StoredProcedure;
						_sqlComm.CommandText = strSP;
						_sqlComm.CommandTimeout	= QUERY_TIMEOUT;

						foreach (SqlParameter aPa in _SQLSPparams)
						{
							_sqlComm.Parameters.Add(SQLCopyParameter(aPa));
						}
						SqlDataAdapter dtaDataAdapter = new SqlDataAdapter(_sqlComm);
						dtaDataAdapter.Fill(dtRet);
						dtaDataAdapter.Dispose();

						binRet = dtRet.Rows[0][0] as byte[];
					}
				} catch (Exception ex) { 
					ReportError("ClsDAL.cs", "SQLGetSPDataTable", strSP, ex); dtRet = null; 
				}

				if (KeepConnectionOpen)
					StopQueryTimer();
				else
					SQLCloseConnection();

				return binRet;
			}
			private void      SQLExecuteSP(string strSP)
			{
				if (!SQLisConnected)
					return;

				try
				{
					_sqlComm = new SqlCommand(strSP, _sqlConn);
					using (_sqlConn)
					{
						_sqlComm.CommandType		= CommandType.StoredProcedure;
						_sqlComm.CommandText		= strSP;
						_sqlComm.CommandTimeout = QUERY_TIMEOUT;

						foreach (SqlParameter aPa in _SQLSPparams)
						{
							_sqlComm.Parameters.Add(SQLCopyParameter(aPa));
						}
						_sqlComm.ExecuteNonQuery();
					}
				} catch (Exception ex) { 
					ReportError("ClsDAL.cs", "SQLExecuteSP", strSP, ex); 
				}

				if (KeepConnectionOpen)
					StopQueryTimer();
				else
					SQLCloseConnection();
			}

		#endregion

		#region "STORED PROCEDURE UPDATE FUNCTIONS"

			private string  SQLUpdateSPDataTable(string strSP, string  strPass)
			{
				string strRet = "";
				string strParamName = "";

				if (!SQLisConnected)
					return strRet;

				try
				{
					_sqlComm = new SqlCommand(strSP, _sqlConn);
					using (_sqlConn)
					{
						_sqlComm.CommandType = CommandType.StoredProcedure;
						_sqlComm.CommandText = strSP;
						_sqlComm.CommandTimeout	= QUERY_TIMEOUT;

						foreach (SqlParameter aPa in _SQLSPparams)
						{
							_sqlComm.Parameters.Add(SQLCopyParameter(aPa));
							if (aPa.Direction == ParameterDirection.Output)
								strParamName = aPa.ParameterName.ToString();
						}
						_sqlComm.ExecuteNonQuery();
						if (strParamName != "")
							strRet = _sqlComm.Parameters[strParamName].Value.ToString();
						else
							strRet = "";
					}
				} catch (Exception ex) { ReportError("ClsDAL.cs", "SQLUpdateSPDataTable(STRING)", strSP, ex); strRet = ""; }

				if (KeepConnectionOpen)
					StopQueryTimer();
				else
					SQLCloseConnection();

				return strRet;
			}
			private int     SQLUpdateSPDataTable(string strSP, int     intPass)
			{
				string strParamName = "";
				string strRet = "";
				int intRet = 0;

				if (!SQLisConnected)
					return intRet;

				try
				{
					_sqlComm = new SqlCommand(strSP, _sqlConn);
					using (_sqlConn)
					{
						_sqlComm.CommandType = CommandType.StoredProcedure;
						_sqlComm.CommandText = strSP;
						_sqlComm.CommandTimeout	= QUERY_TIMEOUT;

						foreach (SqlParameter aPa in _SQLSPparams)
						{
							_sqlComm.Parameters.Add(SQLCopyParameter(aPa));
							if (aPa.Direction == ParameterDirection.Output)
								strParamName = aPa.ParameterName.ToString();
						}
						_sqlComm.ExecuteNonQuery();
						if (strParamName != "")
							strRet = _sqlComm.Parameters[strParamName].Value.ToString();
						else
							strRet = "0";
						intRet = int.Parse(strRet);
					}
				} catch (Exception ex) { ReportError("ClsDAL.cs", "SQLUpdateSPDataTable(INT)", strSP, ex); intRet = 0; }

				if (KeepConnectionOpen)
					StopQueryTimer();
				else
					SQLCloseConnection();

				return intRet;
			}
			private decimal SQLUpdateSPDataTable(string strSP, decimal decPass)
			{
				string strParamName = "";
				string strRet = "";
				decimal decRet = 0;

				if (!SQLisConnected)
					return decRet;

				try
				{
					_sqlComm = new SqlCommand(strSP, _sqlConn);
					using (_sqlConn)
					{
						_sqlComm.CommandType = CommandType.StoredProcedure;
						_sqlComm.CommandText = strSP;
						_sqlComm.CommandTimeout	= QUERY_TIMEOUT;

						foreach (SqlParameter aPa in _SQLSPparams)
						{
							_sqlComm.Parameters.Add(SQLCopyParameter(aPa));
							if (aPa.Direction == ParameterDirection.Output)
								strParamName = aPa.ParameterName.ToString();
						}
						_sqlComm.ExecuteNonQuery();
						if (strParamName != "")
							strRet = _sqlComm.Parameters[strParamName].Value.ToString();
						else
							strRet = "0";
						decRet = decimal.Parse(strRet);
					}
				} catch (Exception ex) { ReportError("ClsDAL.cs", "SQLUpdateSPDataTable(DECIMAL)", strSP, ex); decRet = 0; }

				if (KeepConnectionOpen)
					StopQueryTimer();
				else
					SQLCloseConnection();

				return decRet;
			}

		#endregion

		#region "BULKCOPY / INSERT"

					private IEnumerator		SQLBulkCopier(string strTableName, DataTable dtTable)
					{
						_blnIsProcessing = true;

						#if IS_DEBUGGING
						UnityEngine.Debug.Log("Inside the SQLBulkCopier (" + strTableName + ")! " + DateTime.Now.ToString());
						#endif
						
						bool blnWriteSuccess	= true;
						if (IsConnected)
						{
							using (SqlBulkCopy bulkCopy = new SqlBulkCopy(_sqlConn))
							{
								bulkCopy.DestinationTableName	= "dbo." + strTableName;
				        bulkCopy.SqlRowsCopied			 += new SqlRowsCopiedEventHandler(SQLRowsCopied);
								bulkCopy.BulkCopyTimeout			= QUERY_TIMEOUT; // in seconds
								bulkCopy.BatchSize						= 100;
								bulkCopy.NotifyAfter					= 100;
								try
								{
									bulkCopy.WriteToServer(dtTable, DataRowState.Modified);
								} catch {
									blnWriteSuccess = false;
								}
							}
						}
							
						if (!blnWriteSuccess)
						{
							if (IsConnected)
							{
								SqlCommand	command = _sqlConn.CreateCommand();
								SqlTransaction tran = _sqlConn.BeginTransaction();
								command.Transaction = tran;
								string strStart		= "INSERT INTO " + strTableName + " ";
								string strFields	= "";
								for (int i = 0; i < dtTable.Columns.Count; i++)
									strFields += "," + dtTable.Columns[i].ColumnName;
								strFields = "(" + strFields.Substring(1) + ") VALUES (";
								for (int i = 0; i < dtTable.Rows.Count; i++)
								{
									string strData = "";
									for (int d = 0; d < dtTable.Columns.Count; d++)
									{
										if (dtTable.Columns[d].DataType == typeof(string)||
												dtTable.Columns[d].DataType == typeof(DateTime))
												strData += ",'" + dtTable.Rows[i][d].ToString() + "'";
										else
												strData += "," + dtTable.Rows[i][d].ToString();
									}
									strData = strData.Substring(1) + ")";
									command.CommandText = strStart + strFields + strData;
									command.ExecuteNonQuery();
								}
								yield return null;
								#if IS_DEBUGGING
								UnityEngine.Debug.Log("Start Commit (" + dtTable.Rows.Count.ToString() + " Rows) " + DateTime.Now.ToString());
								#endif
								yield return null;
								try
								{
									tran.Commit();
								} catch (Exception ex) { 
									ReportError("ClsDAL", "SQLBulkCopier", "", ex);
									tran.Rollback();
								}
							} else {
								_strErrors = "NOT CONNECTED!";
							}
						}

						_blnIsProcessing = false;

						if (KeepConnectionOpen)
							StopQueryTimer();
						else
							SQLCloseConnection();
					}
					static	void		SQLRowsCopied(object sender, SqlRowsCopiedEventArgs e)
					{
						Console.WriteLine("-- Copied {0} rows.", e.RowsCopied);
					}
					private bool		SQLInsertFromDataTable(string strSP, string strDataType, DataTable dtTable)
					{
						if (!SQLisConnected)
							return false;

						try
						{
							_sqlComm = new SqlCommand(strSP, _sqlConn);
							using (_sqlConn)
							{
								_sqlComm.CommandType = CommandType.StoredProcedure;
								_sqlComm.CommandText = strSP;

								SqlParameter parameter = new SqlParameter();
								parameter.ParameterName = "@" + strDataType;
//							parameter.SqlDbType = System.Data.SqlDbType.Structured;
								parameter.Value = dtTable;
//							parameter.TypeName = strDataType;
								_sqlComm.Parameters.Add(parameter);

								_sqlComm.ExecuteNonQuery();
								return true;
							}
						} catch (Exception ex) {
							ReportError("ClsDAL.cs", "SQLInsertFromDataTable", strSP, ex);
							_strErrors = ex.Message + "\n" + ex.InnerException + "\n" + ex.StackTrace;
						}

						if (KeepConnectionOpen)
							StopQueryTimer();
						else
							SQLCloseConnection();

						return false;
					}

		#endregion

		#region "DATABASE BACKUP FUNCTIONS"

			private bool    SQLBackupDatabase(string strDatabase, string strBackupPath)
			{
				if (!SQLisConnected)
					return false;

				// ABORT IF THERE IS NO DATABASE FOR THIS WEB SITE
				if (strDatabase == "" || strBackupPath == "") 
					return false;

				try
				{
					ClearParams();
					AddParam("DBNAME",  strDatabase);
					AddParam("DIRNAME", strBackupPath);
					ExecuteSP("spCmdBackupDatabase");

					/*
					// BUILD THE BACKUP COMMAND MANUALLY, NOT THROUGH THE ABOVE STORED PROCEDURE
					SqlCommand   sCom  = new SqlCommand();
					string strCommand  = "BACKUP DATABASE ";
								 strCommand += strDatabase + " TO DISK = '" + strBackupPath + "' WITH NOFORMAT, NOINIT, NAME = 'Full " + strDatabase + " Database Backup', SKIP, NOREWIND, NOUNLOAD,STATS = 10";

					// PERFORM THE SQL COMMAND TO BACKUP THE DATABASE TO THE FILE
					SQLOpenConnection();
					sCom  = new SqlCommand(strCommand, sqlConn);
					sCom.CommandType = CommandType.Text;

					sCom.ExecuteNonQuery();
					sCom.Dispose();
					*/

					return true;
				} catch (System.Exception ex) {
					ReportError("ClsDAL.cs", "SQLBackupDatabase", "DB: " + strDatabase + ", PATH: " + strBackupPath, ex);
					return false;
				}
			} 
		
		#endregion
				
	#endregion

	#region "MYSQL CODE"

		#region "CONNECTION FUNCTIONS"

			private string				MySQLConnectionString
			{
				get
				{
					return "server=" + DBserverString + ";database=" + DBdatabase + ";user=" + DBuser + ";password=" + DBpassword + ((DBport > 0) ? (";port=" + DBport.ToString()) : "");
				}
			}

			private void					MySQLOpenConnection()
			{
				if (_blnIsConnecting)
						return;

				try
				{
					StartQueryTimer();
					Job.make(MySQLOpenConnectionEnum(), true);
				} catch {
					StopQueryTimer();
					_blnIsConnecting	= false;
					_blnIsOnline			= false;
					_blnIsFailed			= true;
				}
			}
			private IEnumerator		MySQLOpenConnectionEnum()
			{
				_blnIsOnline			= false;
				_blnIsFailed			= false;
				_blnIsConnecting	= true;

				if (_mySqlConn == null) 
				{
					_mySqlConn = new MySqlConnection(MySQLConnectionString);
					_mySqlConn.Open();
				} else {
					switch (_mySqlConn.State) 
					{
					case ConnectionState.Open:
							_blnIsOnline = true;
							break;
						case ConnectionState.Closed:
							_mySqlConn.Dispose();
							_mySqlConn = new MySqlConnection(MySQLConnectionString);
							try 
							{  
								_mySqlConn.Open(); 
							} catch (System.Exception ex) { 
								_blnIsFailed = true;
								ReportError("ClsDAL", "MySQLOpenConnectionEnum", "The Database does not Exist", ex);
							}
							break;
						case ConnectionState.Broken:
							_mySqlConn.Close();
							_mySqlConn.Open();
							break;
					}
				}
				Util.Timer clock = new Util.Timer();
				clock.StartTimer();
				while (!_blnIsOnline && clock.GetTime < CONNECTION_TIMEOUT)
				{
					yield return null;
					_blnIsOnline = (_mySqlConn.State != ConnectionState.Broken && _mySqlConn.State != ConnectionState.Closed);
				}
				clock.StopTimer();
				_mySqlConn.StateChange += OnMySQLstateChanged;
				_blnIsConnecting	= false;
				_blnIsFailed			= !_blnIsOnline;
				#if USES_STATUSMANAGER
				if (StatusManager.Instance != null)
						StatusManager.Instance.UpdateStatus();
				#endif
			}
			private void					MySQLOpenConnection(string strServer, string strDB, string strUser, string strPwd, int intPort = 0)
			{
				DBserver		= strServer;
				DBdatabase	= strDB;
				DBuser			= strUser;
				DBpassword	= strPwd;
				DBport			= intPort;
				MySQLOpenConnection();
			}

			private void					MySQLCloseConnection()
			{
				StopQueryTimer();
				_blnIsConnecting	= false;
				_blnIsOnline			= false;
				_blnIsProcessing	= false;

				if (_mySqlConn == null || !_blnIsOnline)
					return;
				else
				{
					if (_mySqlComm != null)
							_mySqlComm.Dispose();
					_mySqlConn.Close();
					_mySqlConn.Dispose();
				}
				#if USES_STATUSMANAGER
				if (StatusManager.Instance != null)
						StatusManager.Instance.UpdateStatus();
				#endif
			}
			private bool					MySQLisConnected
			{
				get
				{
					if (!_blnIsConnecting && (_mySqlConn == null || _mySqlConn.State == ConnectionState.Broken || _mySqlConn.State == ConnectionState.Closed))
					{ 
						if (_mySqlConn == null)
						{
							_mySqlConn = new MySqlConnection(MySQLConnectionString);
							_mySqlConn.Open();
						} else if (_mySqlConn.State == ConnectionState.Broken || _mySqlConn.State == ConnectionState.Closed) {
							_mySqlConn.Close();
							_mySqlConn.Dispose();
							_mySqlConn = new MySqlConnection(MySQLConnectionString);
							_mySqlConn.Open();
						}
					}
					_blnIsOnline = (_mySqlConn != null && _mySqlConn.State != ConnectionState.Broken && _mySqlConn.State != ConnectionState.Closed && !_blnIsConnecting);
					if (_blnIsOnline)
						StartQueryTimer();
					#if USES_STATUSMANAGER
					if (StatusManager.Instance != null)
						StatusManager.Instance.UpdateStatus();
					#endif
					return _blnIsOnline;
				}
			}
			private bool					MySQLisConnecting
			{
				get
				{
					return  _blnIsConnecting;
				}
			}

			private void					OnMySQLstateChanged(object conn, System.Data.StateChangeEventArgs e)
			{
				_blnIsOnline = (_mySqlConn != null && _mySqlConn.State != ConnectionState.Broken && _mySqlConn.State != ConnectionState.Closed && !_blnIsConnecting);
				#if USES_STATUSMANAGER
				if (StatusManager.Instance != null)
						StatusManager.Instance.UpdateStatus();
				#endif
			}

		#endregion

		#region "PARAMETER DEFINITION FUNCTIONS"

			private void MySQLRemoveDuplicateParameter(string strParamName)
			{
				strParamName = strParamName.ToLower();
				for (int i = _mySqlParams.Count - 1; i >= 0; i--)
				{
					if (_mySqlParams[i].ParameterName.ToLower() == strParamName)
					{
						_mySqlParams.RemoveAt(i);
						break;
					}
				}
			}

			private void MySQLAddParam(string strParamName, string    strParamValue)
			{
				if (!MySQLisConnected)
					return;
							 
				try
				{
					MySQLRemoveDuplicateParameter(strParamName);
					MySqlParameter sParam	= new MySqlParameter();
					sParam.ParameterName	= strParamName;
					sParam.DbType					= DbType.String;
					sParam.MySqlDbType		= MySqlDbType.String;
					sParam.Value					= strParamValue;
					sParam.Direction			= ParameterDirection.Input;
					_mySqlParams.Add(sParam);
				} catch (Exception ex) { ReportError("ClsDAL.cs", "MySQLAddParam(STRING)", strParamValue, ex); }
			}
			private void MySQLAddParam(string strParamName, int       intParamValue)
			{
				if (!MySQLisConnected)
					return;

				try
				{
					MySQLRemoveDuplicateParameter(strParamName);
					MySqlParameter sParam	= new MySqlParameter();
					sParam.ParameterName	= strParamName;
					sParam.DbType					= DbType.Int32;
					sParam.MySqlDbType		= MySqlDbType.Int32;
					sParam.Value					= intParamValue;
					sParam.Direction			= ParameterDirection.Input;
					_mySqlParams.Add(sParam);
				} catch (Exception ex) { ReportError("ClsDAL.cs", "MySQLAddParam(INT)", intParamValue.ToString(), ex); }
			}
			private void MySQLAddParam(string strParamName, decimal   decParamValue)
			{
				if (!MySQLisConnected)
					return;

				try 
				{
					MySQLRemoveDuplicateParameter(strParamName);
					MySqlParameter sParam	= new MySqlParameter();
					sParam.ParameterName	= strParamName;
					sParam.DbType					= DbType.Decimal;
					sParam.MySqlDbType		= MySqlDbType.Decimal;
					sParam.Value					= decParamValue;
					sParam.Direction			= ParameterDirection.Input;
					_mySqlParams.Add(sParam);
				} catch (Exception ex) { ReportError("ClsDAL.cs", "MySQLAddParam(DECIMAL)", decParamValue.ToString(), ex); }
			}
			private void MySQLAddParam(string strParamName, float     fParamValue)
			{
				if (!MySQLisConnected)
					return;

				try 
				{
					MySQLRemoveDuplicateParameter(strParamName);
					MySqlParameter sParam = new MySqlParameter();
					sParam.ParameterName	= strParamName;
					sParam.DbType					= DbType.Double;
					sParam.MySqlDbType		= MySqlDbType.Double;
					sParam.Value					= fParamValue;
					sParam.Direction			= ParameterDirection.Input;
					_mySqlParams.Add(sParam);
				} catch (Exception ex) { ReportError("ClsDAL.cs", "MySQLAddParam(FLOAT)", fParamValue.ToString(), ex); }
			}
			private void MySQLAddParam(string strParamName, double    decParamValue)
			{
				if (!MySQLisConnected)
					return;

				try
				{
					MySQLRemoveDuplicateParameter(strParamName);
					MySqlParameter  sParam	= new MySqlParameter();
					sParam.ParameterName		= strParamName;
					sParam.DbType						= DbType.Double;
					sParam.MySqlDbType			= MySqlDbType.Double;
					sParam.Value						= decParamValue;
					sParam.Direction				= ParameterDirection.Input;
					_mySqlParams.Add(sParam);
				} catch (Exception ex) { ReportError("ClsDAL.cs", "MySQLAddParam(DOUBLE)", decParamValue.ToString(), ex); }
			}
			private void MySQLAddParam(string strParamName, long      lngParamValue)
			{
				if (!MySQLisConnected)
					return;

				try
				{
					MySQLRemoveDuplicateParameter(strParamName);
					MySqlParameter sParam = new MySqlParameter();
					sParam.ParameterName	= strParamName;
					sParam.DbType					= DbType.Int64;
					sParam.MySqlDbType		= MySqlDbType.Int64;
					sParam.Value					= lngParamValue;
					sParam.Direction			= ParameterDirection.Input;
					_mySqlParams.Add(sParam);
				} catch (Exception ex) { ReportError("ClsDAL.cs", "MySQLAddParam(LONG)", lngParamValue.ToString(), ex); }
			}
			private void MySQLAddParam(string strParamName, DateTime  dateParamValue)
			{
				if (!MySQLisConnected)
					return;

				try
				{
					MySQLRemoveDuplicateParameter(strParamName);
					MySqlParameter sParam	= new MySqlParameter();
					sParam.ParameterName	= strParamName;
					sParam.DbType					= DbType.DateTime;
					sParam.MySqlDbType		= MySqlDbType.DateTime;
					sParam.Value					= dateParamValue;
					sParam.Direction			= ParameterDirection.Input;
					_mySqlParams.Add(sParam);
				} catch (Exception ex) { ReportError("ClsDAL.cs", "MySQLAddParam(DATE)", dateParamValue.ToString(), ex); }
			}
			private void MySQLAddParam(string strParamName, bool      blnParamValue)
			{
				if (!MySQLisConnected)
					return;

				try
				{
					MySQLRemoveDuplicateParameter(strParamName);
					MySqlParameter sParam	= new MySqlParameter();
					sParam.ParameterName	= strParamName;
					sParam.DbType					= DbType.Boolean;
					sParam.MySqlDbType		= MySqlDbType.Bit;
					sParam.Value		      = blnParamValue;
					sParam.Direction			= ParameterDirection.Input;
					_mySqlParams.Add(sParam);
				} catch (Exception ex) { ReportError("ClsDAL.cs", "MySQLAddParam(BOOLEAN)", blnParamValue.ToString(), ex); }
			}
			private void MySQLAddParam(string strParamName, byte[]		buffer)
			{
				if (!MySQLisConnected)
					return;

				try
				{
					MySQLRemoveDuplicateParameter(strParamName);
					MySqlParameter sParam	= new MySqlParameter();
					sParam.ParameterName	= strParamName;
					sParam.DbType					= DbType.Object;
					sParam.MySqlDbType		= MySqlDbType.VarBinary;
					sParam.Value					= buffer;
					sParam.Direction			= ParameterDirection.Input;
					_mySqlParams.Add(sParam);
				} catch (Exception ex) { ReportError("ClsDAL.cs", "MySQLAddParam(BYTE[])", "...", ex); }
			}
			private void MySQLAddParam(string strParamName, MySqlDbType sType)
			{
				if (!MySQLisConnected)
					return;

				try
				{
					MySQLRemoveDuplicateParameter(strParamName);
					MySqlParameter sParam	= new MySqlParameter();
					sParam.ParameterName	= strParamName;
					sParam.MySqlDbType		= sType;
					sParam.Value					= null;
					sParam.Direction			= ParameterDirection.Output;
					_mySqlParams.Add(sParam);
				} catch (Exception ex) { ReportError("ClsDAL.cs", "MySQLAddParam(OUTPUT)", "", ex); }
			}
			private void MySQLAddParam(string strParamName, MySqlDbType sType, int varSize)
			{
				if (!MySQLisConnected)
					return;

				try
				{
					MySQLRemoveDuplicateParameter(strParamName);
					MySqlParameter sParam	= new MySqlParameter();
					sParam.ParameterName	= strParamName;
					sParam.MySqlDbType		= sType;
					sParam.Size						= varSize;
					sParam.Value					= null;
					sParam.Direction			= ParameterDirection.Output;
					_mySqlParams.Add(sParam);
				} catch (Exception ex) { ReportError("ClsDAL.cs", "MySQLAddParam(OUTPUT,SIZE)", "", ex); }
			}
			private void MySQLClearParams()
			{
				if (!MySQLisConnected && !_blnIsConnecting)
				{
					MySQLOpenConnection();
					if (!MySQLisConnected)
					{
						_strErrors += "Connection to Database Lost.\n"; 
						return;
					}
				}

				try
				{
					if (_mySqlParams != null)
							_mySqlParams.Clear();
				} catch (Exception ex) { ReportError("ClsDAL.cs", "MySQLClearParams", "", ex); }
				_mySqlComm		= new MySqlCommand();
				_mySqlParams	= _mySqlComm.Parameters;
				try
				{
					if (_mySqlParams != null)
							_mySqlParams.Clear();
				} catch (Exception ex) { ReportError("ClsDAL.cs", "MySQLClearParams", "", ex); }
			}
			private MySqlParameter	MySQLCopyParameter(MySqlParameter aPa)
			{
				MySqlParameter	olt = new MySqlParameter();
				olt.DbType					= aPa.DbType;
				olt.Direction				= aPa.Direction;
				olt.DbType					= aPa.DbType;
				olt.MySqlDbType			= aPa.MySqlDbType;
				olt.ParameterName		= aPa.ParameterName;
				olt.Size						= aPa.Size;
				olt.Value						= aPa.Value;
				return olt;
			}

		#endregion

		#region "DIRECT SQL SELECT FUNCTIONS"

			private DataTable MySQLGetSQLSelectDataTable(	string strSQL)
			{
				DataTable dtRet = new DataTable();

				if (!MySQLisConnected)
					return null;
						
				try
				{
					MySqlDataAdapter dtaDataAdapter = new MySqlDataAdapter(strSQL, _mySqlConn);
					dtaDataAdapter.Fill(dtRet);
					dtaDataAdapter.Dispose();
				} catch (Exception ex) { ReportError("ClsDAL.cs", "MySQLGetSQLSelectDataTable", strSQL, ex); dtRet = null; }

				if (KeepConnectionOpen)
					StopQueryTimer();
				else
					MySQLCloseConnection();

				return dtRet;
			}
			private string    MySQLGetSQLSelectString(		string strSQL)
			{
				string strRet = "";
				_sqlComm = null;

				if (!MySQLisConnected)
					return strRet;

				try
				{
					_mySqlComm = _mySqlConn.CreateCommand();
					_mySqlComm.CommandText		= strSQL;
					_mySqlComm.CommandTimeout = QUERY_TIMEOUT;
					try 
					{
						strRet = _mySqlComm.ExecuteScalar().ToString(); 
					} catch (Exception ex) { ReportError("ClsDAL.cs", "MySQLGetSQLSelectString", "Unable to Extract Scalar", ex); strRet = ""; }
				} catch (Exception ex) {
					ReportError("ClsDAL.cs", "MySQLGetSQLSelectString", strSQL, ex);
					strRet = "";
				}

				if (KeepConnectionOpen)
					StopQueryTimer();
				else
					MySQLCloseConnection();

				return strRet;
			}
			private int       MySQLGetSQLSelectInt(				string strSQL)
			{
				int intRet = 0;

				if (!MySQLisConnected)
					return intRet;

				string strTemp = MySQLGetSQLSelectString(strSQL);
				try
				{
					intRet = int.Parse(strTemp);
				} catch {
					intRet = 0;
				}

				return intRet;
			}
			private decimal   MySQLGetSQLSelectDecimal(		string strSQL)
			{
				decimal decRet = 0;

				if (!MySQLisConnected)
					return decRet;

				string strTemp = MySQLGetSQLSelectString(strSQL);
				try
				{
					decRet = decimal.Parse(strTemp);
				} catch {
					decRet = 0;
				}

				return decRet;
			}
			private float			MySQLGetSQLSelectFloat(			string strSQL)
			{
				float fRet = 0;

				if (!MySQLisConnected)
					return fRet;

				string strTemp = MySQLGetSQLSelectString(strSQL);
				try
				{
					fRet = float.Parse(strTemp);
				} catch {
					fRet = 0;
				}

				return fRet;
			}
			private double    MySQLGetSQLSelectDouble(		string strSQL)
			{
				double dblRet = 0;

				if (!MySQLisConnected)
					return dblRet;

				string strTemp = MySQLGetSQLSelectString(strSQL);
				try
				{
					dblRet = double.Parse(strTemp);
				} catch {
					dblRet = 0;
				}

				return dblRet;
			}
			private bool      MySQLDoSQLUpdateDelete(			string strSQL)
			{
				bool blnRet = false;

				if (!MySQLisConnected)
					return blnRet;

				_mySqlComm = _mySqlConn.CreateCommand();
				_mySqlComm.CommandText		= strSQL;
				_mySqlComm.CommandTimeout = QUERY_TIMEOUT;
				_mySqlComm.Prepare();

				try
				{
					blnRet = (_mySqlComm.ExecuteNonQuery() > 0);
				} catch (Exception ex) {
					ReportError("ClsDAL.cs", "MySQLDoSQLUpdateDelete", strSQL, ex);
					blnRet = false; 
				}

				if (KeepConnectionOpen)
					StopQueryTimer();
				else
					MySQLCloseConnection();

				return blnRet;
			}

		#endregion

		#region "STORED PROCEDURE SELECT FUNCTIONS"

			private DataTable MySQLGetSPDataTable(string strSP)
			{
				DataTable dtRet = new DataTable();

				if (!MySQLisConnected)
					return null;

				try
				{
					_mySqlComm = new MySqlCommand(strSP, _mySqlConn);
					using (_mySqlConn)
					{
						_mySqlComm.CommandType = CommandType.StoredProcedure;
						_mySqlComm.CommandText = strSP;
						_mySqlComm.CommandTimeout	= QUERY_TIMEOUT;

						foreach (MySqlParameter aPa in _mySqlParams)
						{
							_mySqlComm.Parameters.Add(MySQLCopyParameter(aPa));
						}
						MySqlDataAdapter dtaDataAdapter = new MySqlDataAdapter(_mySqlComm);
						dtaDataAdapter.Fill(dtRet);
						dtaDataAdapter.Dispose();
					}
				} catch (Exception ex) { 
					ReportError("ClsDAL.cs", "MySQLGetSPDataTable", strSP, ex); dtRet = null; 
				}

				if (KeepConnectionOpen)
					StopQueryTimer();
				else
					MySQLCloseConnection();

				return dtRet;
			}
			private string    MySQLGetSPString(string strSP)
			{
				string strRet = "";

				if (!MySQLisConnected)
					return strRet;

				try
				{
					_mySqlComm = new MySqlCommand(strSP, _mySqlConn);
					using (_mySqlConn)
					{
						string strParamName = "";
						_mySqlComm.CommandType = CommandType.StoredProcedure;
						_mySqlComm.CommandText = strSP;
						_mySqlComm.CommandTimeout	= QUERY_TIMEOUT;

						foreach (MySqlParameter sqlpar in _mySqlParams)
						{
							_mySqlComm.Parameters.Add(MySQLCopyParameter(sqlpar));
							if (sqlpar.Direction == ParameterDirection.Output)
								strParamName = sqlpar.ParameterName.ToString();
						}
						if (strParamName != "")
						{
							_mySqlComm.ExecuteNonQuery();
							strRet = _mySqlComm.Parameters[strParamName].Value.ToString();
						} else {
							DataTable dtRet = new DataTable();
							MySqlDataAdapter dtaDataAdapter = new MySqlDataAdapter(_mySqlComm);
							dtaDataAdapter.Fill(dtRet);
							dtaDataAdapter.Dispose();
							try { strRet = dtRet.Rows[0][0].ToString(); } catch { strRet = ""; }
						}
					}
				} catch (Exception ex) { ReportError("ClsDAL.cs", "MySQLGetSPString", strSP, ex); }

				if (KeepConnectionOpen)
					StopQueryTimer();
				else
					MySQLCloseConnection();

				return strRet;
			}
			private int       MySQLGetSPInt(string strSP)
			{
				int intRet = 0;

				if (!MySQLisConnected)
					return intRet;

				string strTemp = MySQLGetSPString(strSP);
				try
				{
					intRet = int.Parse(strTemp);
				} catch {
					intRet = 0;
				}

				return intRet;
			}
			private long      MySQLGetSPLong(string strSP)
			{
				long lngRet = 0;

				if (!MySQLisConnected)
					return lngRet;

				string strTemp = MySQLGetSPString(strSP);
				try
				{
					lngRet = long.Parse(strTemp);
				} catch {
					lngRet = 0;
				}

				return lngRet;
			}
			private decimal   MySQLGetSPDecimal(string strSP)
			{
				decimal decRet = 0;

				if (!MySQLisConnected)
					return decRet;

				string strTemp = MySQLGetSPString(strSP);
				try
				{
					decRet = decimal.Parse(strTemp);
				} catch {
					decRet = 0;
				}

				return decRet;
			}
			private float		  MySQLGetSPFloat(string strSP)
			{
				float		fRet = 0;

				if (!MySQLisConnected)
					return fRet;

				string strTemp = MySQLGetSPString(strSP);
				try
				{
					fRet = float.Parse(strTemp);
				} catch {
					fRet = 0;
				}

				return fRet;
			}
			private byte[]    MySQLGetSPBinary(string strSP)
			{
				byte[]    binRet       = null;
				DataTable dtRet        = new DataTable();

				if (!MySQLisConnected)
					return binRet;

				try
				{
					_mySqlComm = new MySqlCommand(strSP, _mySqlConn);
					using (_mySqlConn)
					{
						_mySqlComm.CommandType = CommandType.StoredProcedure;
						_mySqlComm.CommandText = strSP;
						_mySqlComm.CommandTimeout	= QUERY_TIMEOUT;

						foreach (MySqlParameter aPa in _mySqlParams)
						{
							_mySqlComm.Parameters.Add(MySQLCopyParameter(aPa));
						}
						MySqlDataAdapter dtaDataAdapter = new MySqlDataAdapter(_mySqlComm);
						dtaDataAdapter.Fill(dtRet);
						dtaDataAdapter.Dispose();

						binRet = dtRet.Rows[0][0] as byte[];
					}
				} catch (Exception ex) { 
					ReportError("ClsDAL.cs", "MySQLGetSPDataTable", strSP, ex); dtRet = null; 
				}

				if (KeepConnectionOpen)
					StopQueryTimer();
				else
					MySQLCloseConnection();

				return binRet;
			}
			private void      MySQLExecuteSP(string strSP)
			{
				if (!MySQLisConnected)
					return;

				try
				{
					_mySqlComm = new MySqlCommand(strSP, _mySqlConn);
					using (_mySqlConn)
					{
						_mySqlComm.CommandType		= CommandType.StoredProcedure;
						_mySqlComm.CommandText		= strSP;
						_mySqlComm.CommandTimeout = QUERY_TIMEOUT;

						foreach (MySqlParameter aPa in _mySqlParams)
						{
							_mySqlComm.Parameters.Add(MySQLCopyParameter(aPa));
						}
						_mySqlComm.ExecuteNonQuery();
					}
				} catch (Exception ex) { 
					ReportError("ClsDAL.cs", "MySQLExecuteSP", strSP, ex); 
				}

				if (KeepConnectionOpen)
					StopQueryTimer();
				else
					MySQLCloseConnection();
			}

		#endregion

		#region "STORED PROCEDURE UPDATE FUNCTIONS"

			private string  MySQLUpdateSPDataTable(string strSP, string  strPass)
			{
				string strRet = "";
				string strParamName = "";

				if (!MySQLisConnected)
					return strRet;

				try
				{
					_mySqlComm = new MySqlCommand(strSP, _mySqlConn);
					using (_mySqlConn)
					{
						_mySqlComm.CommandType = CommandType.StoredProcedure;
						_mySqlComm.CommandText = strSP;
						_mySqlComm.CommandTimeout	= QUERY_TIMEOUT;

						foreach (MySqlParameter aPa in _mySqlParams)
						{
							_mySqlComm.Parameters.Add(MySQLCopyParameter(aPa));
							if (aPa.Direction == ParameterDirection.Output)
								strParamName = aPa.ParameterName.ToString();
						}
						_mySqlComm.ExecuteNonQuery();
						if (strParamName != "")
							strRet = _mySqlComm.Parameters[strParamName].Value.ToString();
						else
							strRet = "";
					}
				} catch (Exception ex) { ReportError("ClsDAL.cs", "MySQLUpdateSPDataTable(STRING)", strSP, ex); strRet = ""; }

				if (KeepConnectionOpen)
					StopQueryTimer();
				else
					MySQLCloseConnection();

				return strRet;
			}
			private int     MySQLUpdateSPDataTable(string strSP, int     intPass)
			{
				string strParamName = "";
				string strRet = "";
				int intRet = 0;

				if (!MySQLisConnected)
					return intRet;

				try
				{
					_mySqlComm = new MySqlCommand(strSP, _mySqlConn);
					using (_mySqlConn)
					{
						_mySqlComm.CommandType = CommandType.StoredProcedure;
						_mySqlComm.CommandText = strSP;
						_mySqlComm.CommandTimeout	= QUERY_TIMEOUT;

						foreach (MySqlParameter aPa in _mySqlParams)
						{
							_mySqlComm.Parameters.Add(MySQLCopyParameter(aPa));
							if (aPa.Direction == ParameterDirection.Output)
								strParamName = aPa.ParameterName.ToString();
						}
						_mySqlComm.ExecuteNonQuery();
						if (strParamName != "")
							strRet = _mySqlComm.Parameters[strParamName].Value.ToString();
						else
							strRet = "0";
						intRet = int.Parse(strRet);
					}
				} catch (Exception ex) { ReportError("ClsDAL.cs", "MySQLUpdateSPDataTable(INT)", strSP, ex); intRet = 0; }

				if (KeepConnectionOpen)
					StopQueryTimer();
				else
					MySQLCloseConnection();

				return intRet;
			}
			private decimal MySQLUpdateSPDataTable(string strSP, decimal decPass)
			{
				string strParamName = "";
				string strRet = "";
				decimal decRet = 0;

				if (!MySQLisConnected)
					return decRet;

				try
				{
					_mySqlComm = new MySqlCommand(strSP, _mySqlConn);
					using (_mySqlConn)
					{
						_mySqlComm.CommandType = CommandType.StoredProcedure;
						_mySqlComm.CommandText = strSP;
						_mySqlComm.CommandTimeout	= QUERY_TIMEOUT;

						foreach (MySqlParameter aPa in _mySqlParams)
						{
							_mySqlComm.Parameters.Add(MySQLCopyParameter(aPa));
							if (aPa.Direction == ParameterDirection.Output)
								strParamName = aPa.ParameterName.ToString();
						}
						_mySqlComm.ExecuteNonQuery();
						if (strParamName != "")
							strRet = _mySqlComm.Parameters[strParamName].Value.ToString();
						else
							strRet = "0";
						decRet = decimal.Parse(strRet);
					}
				} catch (Exception ex) { ReportError("ClsDAL.cs", "MySQLUpdateSPDataTable(DECIMAL)", strSP, ex); decRet = 0; }

				if (KeepConnectionOpen)
					StopQueryTimer();
				else
					MySQLCloseConnection();

				return decRet;
			}

		#endregion

	#endregion

	#region "SQLITE CODE"

		#region "CONNECTION FUNCTIONS"

			private string				DataPath
			{
				get
				{
					#if USES_UNITY
						return UnityEngine.Application.dataPath;
					#else
						string path = System.Reflection.Assembly.GetExecutingAssembly().CodeBase;
						return System.IO.Path.GetDirectoryName(path);
					#endif
				}
			}

			private string				SQLiteConnectionString
			{
				get
				{
					return "URI=file:" + _strSQLiteDBlocation;
				}
				set
				{
					if (value.Trim() != "")
					{
						_strSQLiteDBlocation = value.Trim();
						if (!_strSQLiteDBlocation.StartsWith(DataPath))
						{
							if (!_strSQLiteDBlocation.StartsWith("/"))
									_strSQLiteDBlocation = "/" + _strSQLiteDBlocation;
							_strSQLiteDBlocation =DataPath + _strSQLiteDBlocation;
						}
						if (!Util.FileExists(_strSQLiteDBlocation))
						{
							#if IS_DEBUGGING
							UnityEngine.Debug.LogError("The File '" + _strSQLiteDBlocation + "' does not exist.");
							#endif
							_strSQLiteDBlocation = "";
							return;
						}
						_strSQLiteDBlocation = DataPath + _strSQLiteDBlocation;
					}
				}
			}

			private void					SQLiteOpenConnection()
			{
				// IF ALREADY TRYING TO CONNECT, ABORT
				if (_blnIsConnecting)
					return;

				if (_strSQLiteDBlocation == "")
					return;

				// MAKE SURE THE FILE EXISTS IN THE LOCATION
				if (!Util.FileExists(_strSQLiteDBlocation))
					return;

				try
				{
					StartQueryTimer();
					Job.make(SQLiteOpenConnectionEnum(), true);
				} catch {
					StopQueryTimer();
					_blnIsConnecting	= false;
					_blnIsOnline			= false;
					_blnIsFailed			= true;
				}
			}
			private void					SQLiteOpenConnection(string strDBloc)
			{
				// IF ALREADY TRYING TO CONNECT, ABORT
				if (_blnIsConnecting)
					return;

				// DO NOT ALLOW EMPTY FILE LOCATION
				if (strDBloc.Trim() == "")
					return;

				if (!strDBloc.StartsWith("/"))
						strDBloc = "/" + strDBloc;

				// MAKE SURE THE FILE EXISTS IN THE LOCATION
				if (!Util.FileExists(DataPath + strDBloc.Trim()))
					return;

				// SET THE FILE LOCATION AND OPEN THE CONNECTION
				_strSQLiteDBlocation = DataPath + strDBloc.Trim();
				SQLiteOpenConnection();
			}
			private IEnumerator		SQLiteOpenConnectionEnum()
			{
				_blnIsOnline			= false;
				_blnIsFailed			= false;
				_blnIsConnecting	= true;

				if (_sqlConn == null) 
				{
					_sqliteConn			= (SqliteConnection) new SqliteConnection(SQLiteConnectionString);
					_sqliteConn.Open();
				} else {
					switch (_sqliteConn.State) 
					{
						case ConnectionState.Open:
							_blnIsOnline = true;
							break;
						case ConnectionState.Closed:
							_sqliteConn.Dispose();
							_sqliteConn = new SqliteConnection(SQLiteConnectionString);
							try 
							{  
								_sqliteConn.Open(); 
							} catch (System.Exception ex) { 
								_blnIsFailed = true;
								ReportError("ClsDAL", "SQLiteOpenConnectionEnum", "The Database does not Exist", ex);
							}
							break;
						case ConnectionState.Broken:
							_sqliteConn.Close();
							_sqliteConn.Open();
							break;
					}
				}
				Util.Timer clock = new Util.Timer();
				clock.StartTimer();
				while (!_blnIsOnline && clock.GetTime < CONNECTION_TIMEOUT)
				{
					yield return null;
					_blnIsOnline = (_sqliteConn.State != ConnectionState.Broken && _sqliteConn.State != ConnectionState.Closed);
				}
				clock.StopTimer();
				_blnIsConnecting	= false;
				_blnIsFailed			= !_blnIsOnline;
				#if USES_STATUSMANAGER
				if (StatusManager.Instance != null)
						StatusManager.Instance.UpdateStatus();
				#endif
			}

			private void					SQLiteCloseConnection()
			{
				StopQueryTimer();
				_blnIsConnecting	= false;
				_blnIsOnline			= false;
				_blnIsProcessing	= false;

				if (_sqliteConn == null || !_blnIsOnline)
					return;
				else
				{
					if (_sqliteComm != null)
							_sqliteComm.Dispose();
					_sqliteConn.Close();
					_sqliteConn.Dispose();
				}
				#if USES_STATUSMANAGER
				if (StatusManager.Instance != null)
						StatusManager.Instance.UpdateStatus();
				#endif
			}
			private bool					SQLiteIsConnected
			{
				get
				{
					if (!_blnIsConnecting && (_sqliteConn == null || _sqliteConn.State == ConnectionState.Broken || _sqliteConn.State == ConnectionState.Closed))
					{ 
						if (_sqliteConn == null)
						{
							_sqliteConn = new SqliteConnection(SQLiteConnectionString);
							_sqliteConn.Open();
						} else if (_sqliteConn.State == ConnectionState.Broken || _sqliteConn.State == ConnectionState.Closed) {
							_sqliteConn.Close();
							_sqliteConn.Dispose();
							_sqliteConn = new SqliteConnection(SQLiteConnectionString);
							_sqliteConn.Open();
						}
					}
					_blnIsOnline = (_sqliteConn != null && _sqliteConn.State != ConnectionState.Broken && _sqliteConn.State != ConnectionState.Closed && !_blnIsConnecting);
					if (_blnIsOnline)
						StartQueryTimer();
					#if USES_STATUSMANAGER
					if (StatusManager.Instance != null)
						StatusManager.Instance.UpdateStatus();
					#endif
					return _blnIsOnline;
				}
			}
			private bool					SQLiteIsConnecting
			{
				get
				{
					return  _blnIsConnecting;
				}
			}

		#endregion

		#region "DIRECT SQL SELECT FUNCTIONS"

			private DataTable SQLiteGetSQLSelectDataTable(	string strSQL)
			{
				DataTable dtRet = new DataTable();

				if (!SQLiteIsConnected)
					return null;
						
				try
				{
					_sqliteComm = new SqliteCommand(_sqliteConn);
					_sqliteComm.CommandText			= strSQL;
					_sqliteComm.CommandTimeout	= QUERY_TIMEOUT;

					try
					{
						SqliteDataAdapter adapter = new SqliteDataAdapter(_sqliteComm);
						adapter.Fill(dtRet);
						adapter.Dispose();
					} catch (Exception ex) { ReportError("ClsDAL.cs", "SQLiteGetSQLSelectDataTable", strSQL, ex); dtRet = null; }
				} catch (Exception ex) {
					ReportError("ClsDAL.cs", "SQLiteGetSQLSelectDataTable", strSQL, ex);
					dtRet = null;
				}

				if (KeepConnectionOpen)
					StopQueryTimer();
				else
					SQLiteCloseConnection();

				return dtRet;
			}
			private string    SQLiteGetSQLSelectString(		string strSQL)
			{
				string strRet = "";
				_sqliteComm = null;

				if (!SQLiteIsConnected)
					return strRet;

				try
				{
					_sqliteComm = _sqliteConn.CreateCommand();
					_sqliteComm.CommandText			= strSQL;
					_sqliteComm.CommandTimeout	= QUERY_TIMEOUT;
					try 
					{
						strRet = _sqliteComm.ExecuteScalar().ToString(); 
					} catch (Exception ex) { ReportError("ClsDAL.cs", "SQLiteGetSQLSelectString", "Unable to Extract Scalar", ex); strRet = ""; }
				} catch (Exception ex) {
					ReportError("ClsDAL.cs", "SQLiteGetSQLSelectString", strSQL, ex);
					strRet = "";
				}

				if (KeepConnectionOpen)
					StopQueryTimer();
				else
					SQLiteCloseConnection();

				return strRet;
			}
			private int       SQLiteGetSQLSelectInt(				string strSQL)
			{
				int intRet = 0;

				if (!SQLiteIsConnected)
					return intRet;

				string strTemp = SQLiteGetSQLSelectString(strSQL);
				try
				{
					intRet = int.Parse(strTemp);
				} catch {
					intRet = 0;
				}

				return intRet;
			}
			private decimal   SQLiteGetSQLSelectDecimal(		string strSQL)
			{
				decimal decRet = 0;

				if (!SQLiteIsConnected)
					return decRet;

				string strTemp = SQLiteGetSQLSelectString(strSQL);
				try
				{
					decRet = decimal.Parse(strTemp);
				} catch {
					decRet = 0;
				}

				return decRet;
			}
			private float			SQLiteGetSQLSelectFloat(			string strSQL)
			{
				float fRet = 0;

				if (!SQLiteIsConnected)
					return fRet;

				string strTemp = SQLiteGetSQLSelectString(strSQL);
				try
				{
					fRet = float.Parse(strTemp);
				} catch {
					fRet = 0;
				}

				return fRet;
			}
			private double    SQLiteGetSQLSelectDouble(		string strSQL)
			{
				double dblRet = 0;

				if (!SQLiteIsConnected)
					return dblRet;

				string strTemp = SQLiteGetSQLSelectString(strSQL);
				try
				{
					dblRet = double.Parse(strTemp);
				} catch {
					dblRet = 0;
				}

				return dblRet;
			}
			private bool      SQLiteDoSQLUpdateDelete(			string strSQL)
			{
				bool blnRet = false;

				if (!SQLiteIsConnected)
					return blnRet;

				_sqliteComm									= _sqliteConn.CreateCommand();
				_sqliteComm.CommandText			= strSQL;
				_sqliteComm.CommandTimeout	= QUERY_TIMEOUT;

				try
				{
					blnRet = (_sqliteComm.ExecuteNonQuery() > 0);
				} catch (Exception ex) {
					ReportError("ClsDAL.cs", "SQLiteDoSQLUpdateDelete", strSQL, ex);
					blnRet = false; 
				}

				if (KeepConnectionOpen)
					StopQueryTimer();
				else
					SQLiteCloseConnection();

				return blnRet;
			}

		#endregion

	#endregion


	#region "MAIN CODE"

		#region "CLASS CONSTRUCTOR"
			
			public        ClsDAL()
			{
				ResetErrors();
			}
			public				ClsDAL(DBtypes dbt)
			{
				_dbType = dbt;
				#if IS_DEBUGGING
				UnityEngine.Debug.Log("ClsDAL.Init : Set DB Type to " + dbt.ToString());
				#endif
				ResetErrors();
			}
			public				ClsDAL(DBtypes dbt, string strDBfileLocation)
			{
				_dbType = dbt;
				#if IS_DEBUGGING
				UnityEngine.Debug.Log("ClsDAL.Init : Set Database Type to '" + dbt.ToString() + "'");
				#endif
				if (dbt == DBtypes.SQLITE)
				{
					SQLiteConnectionString = strDBfileLocation;
					#if IS_DEBUGGING
					UnityEngine.Debug.Log("ClsDAL.Init : Set SQLite File to '" + _strSQLiteDBlocation + "'");
					#endif
				}
				ResetErrors();
			}

			public string SQLqueries
			{
				get
				{
					return _strSQLqueries;
				}
			}
			public void   Dispose()
			{
				Dispose(true);
				GC.SuppressFinalize(this);
			}
			protected virtual void Dispose(bool disposing)
			{
				if (!_blnIsDisposed)
				{
					if (disposing)
					{
						if (_blnIsOnline || _blnIsConnecting)
							this.CloseConnection();
					}
					_blnIsDisposed = true;
				}
			}

		#endregion

		#region "HTML ENCODE AND DECODE"
		
			private string HTMLencode(string strInput)
			{
				return strInput;
			}
			private string HTMLdecode(string strInput)
			{
				return strInput;
			}
			
		#endregion
		
		#region "TIMER FUNCTIONS"

			private void		StartQueryTimer()
			{
				ResetErrors();
				if (_queryTimer == null)
						_queryTimer = new Util.Timer();
				_queryTimer.StartTimer();
			}
			private void		StopQueryTimer()
			{
				if (_queryTimer != null)
				{
					if (_queryTimer.IsRunning)
						_queryTimer.StopTimer();
					_fQueryLast			= _queryTimer.GetFloatTime;
					_fQueryAverage += _fQueryLast;
					_intQueryCount++;
				}
			}
			public	void		ResetAverage()
			{
				_fQueryAverage = 0;
				_intQueryCount = 0;
			}

			public	float		AverageQueryTime
			{
				get
				{
					if (_intQueryCount == 0 || _fQueryAverage == 0)
						return 0;
					else
						return _fQueryAverage / ((float)_intQueryCount);
				}
			}
			public	float		LastQueryTime
			{ 
				get
				{
					return _fQueryLast;
				}
			}

		#endregion

		#region "CONNECTION FUNCTIONS"

			public void OpenConnection()
			{
				if (_blnIsOnline || _blnIsConnecting)
					return;

				#if IS_DEBUGGING
				_strSQLqueries += "OPEN CONNECTION\n";
				#endif

				switch (_dbType)
				{
					case DBtypes.MSSQL:
						SQLOpenConnection();
						break;
					case DBtypes.MYSQL:
						MySQLOpenConnection();
						break;
					case DBtypes.SQLITE:
						SQLiteOpenConnection();
						break;
				}
				#if USES_STATUSMANAGER
				StatusManager.Instance.UpdateStatus();
				#endif
			}
			public void OpenConnection(string strServer, string strDB, int intPort = 0)
			{
				if (_dbType != DBtypes.MSSQL)
						return;
				SQLOpenConnection(strServer, strDB, intPort);
				#if USES_STATUSMANAGER
				StatusManager.Instance.UpdateStatus();
				#endif
			}
			public void OpenConnection(string strServer, string strDB, string strUser, string strPwd, int intPort = 0)
			{
				switch (_dbType)
				{
					case DBtypes.MSSQL:
						SQLOpenConnection(strServer, strDB, strUser, strPwd, intPort);
						break;
					case DBtypes.MYSQL:
						MySQLOpenConnection(strServer, strDB, strUser, strPwd, intPort);
						break;
				}

				#if USES_STATUSMANAGER
				StatusManager.Instance.UpdateStatus();
				#endif
			}
			public void OpenConnection(string strDBfileLocation)
			{
				if (_dbType != DBtypes.SQLITE)
					return;

				SQLiteOpenConnection(strDBfileLocation);

				#if USES_STATUSMANAGER
				StatusManager.Instance.UpdateStatus();
				#endif
			}

			public void CloseConnection()
			{
				#if IS_DEBUGGING
				_strSQLqueries += "CLOSE CONNECTION\n";
				#endif

				switch (_dbType)
				{
					case DBtypes.MSSQL:
						SQLCloseConnection();
						break;
					case DBtypes.MYSQL:
						MySQLCloseConnection();
						break;
					case DBtypes.SQLITE:
						SQLiteCloseConnection();
						break;
				}

				#if USES_STATUSMANAGER
				StatusManager.Instance.UpdateStatus();
				#endif
			}
			public bool IsConnected
			{
				get
				{
					switch (_dbType)
					{
						case DBtypes.MSSQL:
							return SQLisConnected && !IsConnectionFailed;
						case DBtypes.MYSQL:
							return MySQLisConnected && !IsConnectionFailed;
						case DBtypes.SQLITE:
							return SQLiteIsConnected && !IsConnectionFailed;
					}
					return false;
				}
			}
			public bool IsConnectedCheck
			{
				get
				{
					return _blnIsOnline && !IsConnectionFailed;
				}
			}
			public bool IsConnecting
			{
				get
				{
					switch (_dbType)
					{
						case DBtypes.MSSQL:
							return SQLisConnecting;
						case DBtypes.MYSQL:
							return MySQLisConnecting;
						case DBtypes.SQLITE:
							return SQLiteIsConnecting;
					}
					return false;
				}
			}

		#endregion

		#region "PARAMETER FUNCTIONS"

			public void AddParam(string strParamName, string   strParamValue)
			{
				switch (_dbType)
				{
					case DBtypes.MSSQL:
						SQLAddParam(strParamName, strParamValue);
						break;
					case DBtypes.MYSQL:
						MySQLAddParam(strParamName, strParamValue);
						break;
				}
			}
			public void AddParam(string strParamName, int      intParamValue)
			{
				switch (_dbType)
				{
					case DBtypes.MSSQL:
						SQLAddParam(strParamName, intParamValue);
						break;
					case DBtypes.MYSQL:
						MySQLAddParam(strParamName, intParamValue);
						break;
				}
			}
			public void AddParam(string strParamName, decimal  decParamValue)
			{
				switch (_dbType)
				{
					case DBtypes.MSSQL:
						SQLAddParam(strParamName, decParamValue);
						break;
					case DBtypes.MYSQL:
						MySQLAddParam(strParamName, decParamValue);
						break;
				}
			}
			public void AddParam(string strParamName, float    fParamValue)
			{
				switch (_dbType)
				{
					case DBtypes.MSSQL:
						SQLAddParam(strParamName, fParamValue);
						break;
					case DBtypes.MYSQL:
						MySQLAddParam(strParamName, fParamValue);
						break;
				}
			}
			public void AddParam(string strParamName, double   decParamValue)
			{
				switch (_dbType)
				{
					case DBtypes.MSSQL:
						SQLAddParam(strParamName, decParamValue);
						break;
					case DBtypes.MYSQL:
						MySQLAddParam(strParamName, decParamValue);
						break;
				}
			}
			public void AddParam(string strParamName, long     lngParamValue)
			{
				switch (_dbType)
				{
					case DBtypes.MSSQL:
						SQLAddParam(strParamName, lngParamValue);
						break;
					case DBtypes.MYSQL:
						MySQLAddParam(strParamName, lngParamValue);
						break;
				}
			}
			public void AddParam(string strParamName, DateTime dateParamValue)
			{
				switch (_dbType)
				{
					case DBtypes.MSSQL:
						SQLAddParam(strParamName, dateParamValue);
						break;
					case DBtypes.MYSQL:
						MySQLAddParam(strParamName, dateParamValue);
						break;
				}
			}
			public void AddParam(string strParamName, bool     blnParamValue)
			{
				switch (_dbType)
				{
					case DBtypes.MSSQL:
						SQLAddParam(strParamName, blnParamValue);
						break;
					case DBtypes.MYSQL:
						MySQLAddParam(strParamName, blnParamValue);
						break;
				}
			}
			public void AddParam(string strParamName, byte[]	 buffer)
			{
				switch (_dbType)
				{
					case DBtypes.MSSQL:
						SQLAddParam(strParamName, buffer);
						break;
					case DBtypes.MYSQL:
						MySQLAddParam(strParamName, buffer);
						break;
				}
			}
			public void AddParam(string strParamName, DbType   sType)
			{
				switch (_dbType)
				{
					case DBtypes.MSSQL:
						SqlDbType st = new SqlDbType();
						switch (sType.ToString().ToLower())
						{
							case "int":
							case "int16":
							case "int32":
							case "int64":
								st = SqlDbType.Int;
								break;
							case "float":
							case "double":
							case "decimal":
								st = SqlDbType.Decimal;
								break;
							case "string":
							case "stringfixedlength":
								st = SqlDbType.VarChar;
								break;
							case "date":
							case "time":
							case "datetime":
							case "datetime2":
								st = SqlDbType.DateTime;
								break;
							case "bool":
							case "boolean":
								st = SqlDbType.Bit;
								break;
						}
						AddParam(strParamName, st);
						break;
					case DBtypes.MYSQL:
						MySqlDbType sm = new MySqlDbType();
						switch (sType.ToString().ToLower())
						{
							case "int16":
								sm = MySqlDbType.Int16;
								break;
							case "int":
							case "int32":
								sm = MySqlDbType.Int32;
								break;
							case "int64":
								sm = MySqlDbType.Int64;
								break;
							case "float":
							case "double":
							case "decimal":
								sm = MySqlDbType.Decimal;
								break;
							case "string":
							case "stringfixedlength":
								sm = MySqlDbType.VarChar;
								break;
							case "date":
							case "time":
							case "datetime":
							case "datetime2":
								sm = MySqlDbType.DateTime;
								break;
							case "bool":
							case "boolean":
								sm = MySqlDbType.Bit;
								break;
						}
						AddParam(strParamName, sm);
						break;
				}

			}
			public void AddParam(string strParamName, SqlDbType  sType)
			{
				if (_dbType == DBtypes.MSSQL)
						SQLAddParam(strParamName, sType);
			}
			public void AddParam(string strParamName, SqlDbType  sType, int varSize)
			{
				if (_dbType == DBtypes.MSSQL)
						SQLAddParam(strParamName, sType, varSize);
			}
			public void AddParam(string strParamName, MySqlDbType  sType)
			{
				if (_dbType == DBtypes.MYSQL)
						MySQLAddParam(strParamName, sType);
			}
			public void AddParam(string strParamName, MySqlDbType  sType, int varSize)
			{
				if (_dbType == DBtypes.MYSQL)
						MySQLAddParam(strParamName, sType, varSize);
			}

			public void ClearParams()
			{
				switch (_dbType)
				{
					case DBtypes.MSSQL:
						SQLClearParams();
						break;
					case DBtypes.MYSQL:
						MySQLClearParams();
						break;
				}

				_strSQLqueries = "";

				#if IS_DEBUGGING
				_strSQLqueries = "ClearParams()\n";
				#endif
			}

		#endregion

		#region "DIRECT SQL FUNCTIONS"

			public DataTable GetSQLSelectDataTable(	string strSQL)
			{
				DataTable dtRet = new DataTable();

				#if IS_DEBUGGING
				_strSQLqueries += "-- GetSQLSelectDataTable(" + strSQL + ")\n";
				#endif

				switch (_dbType)
				{
					case DBtypes.MSSQL:
						dtRet = SQLGetSQLSelectDataTable(strSQL);
						break;
					case DBtypes.MYSQL:
						dtRet = MySQLGetSQLSelectDataTable(strSQL);
						break;
					case DBtypes.SQLITE:
						dtRet = SQLiteGetSQLSelectDataTable(strSQL);
						break;
				}

				#if USES_STATUSMANAGER
				StatusManager.Instance.UpdateStatus();
				#endif

				return dtRet;
			}
			public string    GetSQLSelectString(		string strSQL)
			{
				string strRet = "";

				#if IS_DEBUGGING
				_strSQLqueries += "-- GetSQLSelectString(" + strSQL + ")\n";
				#endif
								
				switch (_dbType)
				{
					case DBtypes.MSSQL:
						strRet = SQLGetSQLSelectString(strSQL);
						break;
					case DBtypes.MYSQL:
						strRet = MySQLGetSQLSelectString(strSQL);
						break;
					case DBtypes.SQLITE:
						strRet = SQLiteGetSQLSelectString(strSQL);
						break;
				}

				#if USES_STATUSMANAGER
				StatusManager.Instance.UpdateStatus();
				#endif
				
				return strRet;
			}
			public int       GetSQLSelectInt(				string strSQL)
			{
				int intRet = 0;

				#if IS_DEBUGGING
				_strSQLqueries += "-- GetSQLSelectInt(" + strSQL + ")\n";
				#endif
								
				switch (_dbType)
				{
					case DBtypes.MSSQL:
						intRet = SQLGetSQLSelectInt(strSQL);
						break;
					case DBtypes.MYSQL:
						intRet = MySQLGetSQLSelectInt(strSQL);
						break;
					case DBtypes.SQLITE:
						intRet = SQLiteGetSQLSelectInt(strSQL);
						break;
				}

				#if USES_STATUSMANAGER
				StatusManager.Instance.UpdateStatus();
				#endif

				return intRet;
			}
			public decimal   GetSQLSelectDecimal(		string strSQL)
			{
				decimal decRet = 0;

				#if IS_DEBUGGING
				_strSQLqueries += "-- GetSQLSelectDecimal(" + strSQL + ")\n";
				#endif
								
				switch (_dbType)
				{
					case DBtypes.MSSQL:
						decRet = SQLGetSQLSelectDecimal(strSQL);
						break;
					case DBtypes.MYSQL:
						decRet = MySQLGetSQLSelectDecimal(strSQL);
						break;
					case DBtypes.SQLITE:
						decRet = SQLiteGetSQLSelectDecimal(strSQL);
						break;
				}

				#if USES_STATUSMANAGER
				StatusManager.Instance.UpdateStatus();
				#endif

				return decRet;
			}
			public float     GetSQLSelectFloat(			string strSQL)
			{
				float fRet = 0;

				#if IS_DEBUGGING
				_strSQLqueries += "-- GetSQLSelectFloat(" + strSQL + ")\n";
				#endif

				switch (_dbType)
				{
					case DBtypes.MSSQL:
						fRet = SQLGetSQLSelectFloat(strSQL);
						break;
					case DBtypes.MYSQL:
						fRet = MySQLGetSQLSelectFloat(strSQL);
						break;
					case DBtypes.SQLITE:
						fRet = SQLiteGetSQLSelectFloat(strSQL);
						break;
				}

				#if USES_STATUSMANAGER
				StatusManager.Instance.UpdateStatus();
				#endif

				return fRet;
			}
			public double    GetSQLSelectDouble(		string strSQL)
			{
				double dblRet = 0;

				#if IS_DEBUGGING
				_strSQLqueries += "-- GetSQLSelectDecimal(" + strSQL + ")\n";
				#endif

				switch (_dbType)
				{
					case DBtypes.MSSQL:
						dblRet = SQLGetSQLSelectDouble(strSQL);
						break;
					case DBtypes.MYSQL:
						dblRet = MySQLGetSQLSelectDouble(strSQL);
						break;
					case DBtypes.SQLITE:
						dblRet = SQLiteGetSQLSelectDouble(strSQL);
						break;
				}

				#if USES_STATUSMANAGER
				StatusManager.Instance.UpdateStatus();
				#endif

				return dblRet;
			}
			public bool      DoSQLUpdateDelete(			string strSQL)
			{
				bool blnRet = false;

				#if IS_DEBUGGING
				_strSQLqueries += "-- DoSQLUpdateDelete(" + strSQL + ")\n";
				#endif
								
				switch (_dbType)
				{
					case DBtypes.MSSQL:
						blnRet = SQLDoSQLUpdateDelete(strSQL);
						break;
					case DBtypes.MYSQL:
						blnRet = MySQLDoSQLUpdateDelete(strSQL);
						break;
					case DBtypes.SQLITE:
						blnRet = SQLiteDoSQLUpdateDelete(strSQL);
						break;
				}

				#if USES_STATUSMANAGER
				StatusManager.Instance.UpdateStatus();
				#endif

				return blnRet;
			}

		#endregion

		#region "STORED PROCEDURE SELECT FUNCTIONS"

			public DataTable GetSPDataTable(string strSP)
			{
				DataTable dtRet = new DataTable();

				#if IS_DEBUGGING
				_strSQLqueries += "-- GetSPDataTable(" + strSP + ")\n";
				#endif
				
				switch (_dbType)
				{
					case DBtypes.MSSQL:
						dtRet = SQLGetSPDataTable(strSP);
						break;
					case DBtypes.MYSQL:
						dtRet = MySQLGetSPDataTable(strSP);
						break;
				}		

				#if USES_STATUSMANAGER
				StatusManager.Instance.UpdateStatus();
				#endif

				return dtRet;								
			}
			public string    GetSPString(string strSP)
			{
				string strRet = "";

				#if IS_DEBUGGING
				_strSQLqueries += "-- GetSPString(" + strSP + ")\n";
				#endif
								
				switch (_dbType)
				{
					case DBtypes.MSSQL:
						strRet = SQLGetSPString(strSP);
						break;
					case DBtypes.MYSQL:
						strRet = MySQLGetSPString(strSP);
						break;
				}		

				#if USES_STATUSMANAGER
				StatusManager.Instance.UpdateStatus();
				#endif

				return strRet;
			}
			public int       GetSPInt(string strSP)
			{
				int intRet = 0;

				#if IS_DEBUGGING
				_strSQLqueries += "-- GetSPInt(" + strSP + ")\n";
				#endif

				switch (_dbType)
				{
					case DBtypes.MSSQL:
						intRet = SQLGetSPInt(strSP);
						break;
					case DBtypes.MYSQL:
						intRet = MySQLGetSPInt(strSP);
						break;
				}		

				#if USES_STATUSMANAGER
				StatusManager.Instance.UpdateStatus();
				#endif

				return intRet;
			}
			public long      GetSPLong(string strSP)
			{
				long lngRet = 0;

				#if IS_DEBUGGING
				_strSQLqueries += "-- GetSPInt(" + strSP + ")\n";
				#endif

				switch (_dbType)
				{
					case DBtypes.MSSQL:
						lngRet = SQLGetSPLong(strSP);
						break;
					case DBtypes.MYSQL:
						lngRet = MySQLGetSPLong(strSP);
						break;
				}		

				#if USES_STATUSMANAGER
				StatusManager.Instance.UpdateStatus();
				#endif

				return lngRet;
			}
			public decimal   GetSPDecimal(string strSP)
			{
				decimal decRet = 0;

				#if IS_DEBUGGING
				_strSQLqueries += "-- GetSPDecimal(" + strSP + ")\n";
				#endif

				switch (_dbType)
				{
					case DBtypes.MSSQL:
						decRet = SQLGetSPDecimal(strSP);
						break;
					case DBtypes.MYSQL:
						decRet = MySQLGetSPDecimal(strSP);
						break;
				}		

				#if USES_STATUSMANAGER
				StatusManager.Instance.UpdateStatus();
				#endif

				return decRet;
			}
			public float     GetSPFloat(string strSP)
			{
				float fRet = 0;

				#if IS_DEBUGGING
				_strSQLqueries += "-- GetSPFloat(" + strSP + ")\n";
				#endif

				switch (_dbType)
				{
					case DBtypes.MSSQL:
						fRet = SQLGetSPFloat(strSP);
						break;
					case DBtypes.MYSQL:
						fRet = MySQLGetSPFloat(strSP);
						break;
				}		

				#if USES_STATUSMANAGER
				StatusManager.Instance.UpdateStatus();
				#endif

				return fRet;
			}
			public byte[]    GetSPBinary(string strSP)
			{
				byte[] binRet = null;

				#if IS_DEBUGGING
				_strSQLqueries += "-- GetSPBinary(" + strSP + ")\n";
				#endif

				switch (_dbType)
				{
					case DBtypes.MSSQL:
						binRet = SQLGetSPBinary(strSP);
						break;
					case DBtypes.MYSQL:
						binRet = MySQLGetSPBinary(strSP);
						break;
				}		

				#if USES_STATUSMANAGER
				StatusManager.Instance.UpdateStatus();
				#endif

				return binRet;
			}
			public void      ExecuteSP(string strSP)
			{
				#if IS_DEBUGGING
				_strSQLqueries += "-- ExecuteSP(" + strSP + ")\n";
				#endif

				switch (_dbType)
				{
					case DBtypes.MSSQL:
						SQLExecuteSP(strSP);
						break;
					case DBtypes.MYSQL:
						MySQLExecuteSP(strSP);
						break;
				}		

				#if USES_STATUSMANAGER
				StatusManager.Instance.UpdateStatus();
				#endif
			}

		#endregion

		#region "STORED PROCEDURE UPDATE FUNCTIONS"

			public string  UpdateSPDataTable(string strSP, string  strPass)
			{
				string strRet = "";

				#if IS_DEBUGGING
				_strSQLqueries += "-- UpdateSPDataTable(" + strSP + ")\n";
				#endif

				switch (_dbType)
				{
					case DBtypes.MSSQL:
						strRet = SQLUpdateSPDataTable(strSP, strPass);
						break;
					case DBtypes.MYSQL:
						strRet = MySQLUpdateSPDataTable(strSP, strPass);
						break;
				}		

				#if USES_STATUSMANAGER
				StatusManager.Instance.UpdateStatus();
				#endif

				return strRet;
			}
			public int     UpdateSPDataTable(string strSP, int     intPass)
			{
				int intRet = 0;

				#if IS_DEBUGGING
				_strSQLqueries += "-- UpdateSPDataTable(" + strSP + ")\n";
				#endif

				switch (_dbType)
				{
					case DBtypes.MSSQL:
						intRet = SQLUpdateSPDataTable(strSP, intPass);
						break;
					case DBtypes.MYSQL:
						intRet = MySQLUpdateSPDataTable(strSP, intPass);
						break;
				}		

				#if USES_STATUSMANAGER
				StatusManager.Instance.UpdateStatus();
				#endif

				return intRet;
			}
			public decimal UpdateSPDataTable(string strSP, decimal decPass)
			{
				decimal decRet = 0;

				#if IS_DEBUGGING
				_strSQLqueries += "-- UpdateSPDataTable(" + strSP + ")\n";
				#endif

				switch (_dbType)
				{
					case DBtypes.MSSQL:
						decRet = SQLUpdateSPDataTable(strSP, decPass);
						break;
					case DBtypes.MYSQL:
						decRet = MySQLUpdateSPDataTable(strSP, decPass);
						break;
				}		

				#if USES_STATUSMANAGER
				StatusManager.Instance.UpdateStatus();
				#endif

				return decRet;
			}

		#endregion

		#region "BULKCOPY / INSERT"

			public IEnumerator	BulkCopy(string strTableName, DataTable dtTable)
			{
				#if IS_DEBUGGING
				_strSQLqueries += "-- BulkCopy(" + strTableName + ")\n";
				#endif

				if (_dbType != DBtypes.MSSQL)
					yield break;

				Job.make(SQLBulkCopier(strTableName, dtTable), true);

				yield return null;
			}
			public bool		 InsertFromDataTable(string strSP, string strDataType, DataTable dtTable)
			{
				if (_dbType == DBtypes.MSSQL)
					return SQLInsertFromDataTable(strSP, strDataType, dtTable);
				else
					return false;
			}

		#endregion

		#region "ERROR HANDLING"

			public void   ResetErrors()
			{
				_strErrors = "";
			}
			public string Errors
			{
				get
				{
					return _strErrors;
				}
			}
			public void   ReportError(string strFile, string strFunc, string strPass, Exception ex)
			{
				string strLine = "";
				// ADD ERROR TO THE ERROR MESSAGE LIST
				string strError = "";
				strError		+= "\n";
				strError		+= "--- <b>ERROR in " + strFile + "." + strFunc + "</b> ------------- \n";
				if (strPass != "")
					strError	+= "<b>  Inputs:</b> " + strPass + "\n\n";
				if (strLine != "")
					strError	+= "<b> At Line:</b> " + strLine + "\n";
				strError		+= "<b> Message:</b> " + ex.Message + "\n";
				strError		+= "<b>  Target:</b> " + ex.TargetSite + "\n";
				if (ex.InnerException != null && ex.InnerException.ToString() != "")
					strError	+= "<b>   Inner:</b> " + ex.InnerException + "\n";
				strError		+= "<b>   Trace:</b> " + ex.StackTrace + "\n";
				strError		+= "\n";
				_strErrors	+= strError;
				Util.CopyToClipboard(strError);
			}
			public void   ReportError(string strFile, string strFunc, string strPass, Exception ex, int intUserID)
			{
				string strLine = "";

				// ADD ERROR TO THE ERROR MESSAGE LIST
				string strError = "";
				strError		+= "\n";
				strError		+= "--- <b>ERROR in " + strFile + "." + strFunc + "</b> -------------\n";
				if (strPass != "")
					strError	+= "<b>  Inputs:</b> " + strPass + "\n\n";
				if (strLine != "")
					strError	+= "<b> At Line:</b> " + strLine + "\n";
				strError		+= "<b> Message:</b> " + ex.Message + "\n";
				strError		+= "<b>  Target:</b> " + ex.TargetSite + "\n";
				if (ex.InnerException != null && ex.InnerException.ToString() != "")
					strError	+= "<b>   Inner:</b> " + ex.InnerException + "\n";
				strError		+= "<b>   Trace:</b> " + ex.StackTrace + "\n";
				strError		+= "\n";
				_strErrors	+= strError;
				Util.CopyToClipboard(strError);
			}

		#endregion
		
		#region "DATABASE BACKUP FUNCTIONS"
/*
			public Boolean BackupDatabase(string strBackupPath)
			{
				if (!blnDALonline)
					return false;
					
				try
				{
					// DETERMINE DATABASE NAME
					string strDatabase = "";
					if (ConfigurationManager.AppSettings["db_DB"] != null && ConfigurationManager.AppSettings["db_DB"] != "")
						strDatabase = ConfigurationManager.AppSettings["db_DB"];
					if (strDatabase == "")
						strDatabase = ConfigurationManager.AppSettings["app_name"];

					// SET BACKUP FILE TO DEFAULT IF PASSED STRING IS EMPTY
					if (strBackupPath == "")
						strBackupPath = "~/Database/";
					if (!strBackupPath.EndsWith("/"))
						strBackupPath += "/";

					// MAKE SURE THE DIRECTORY EXISTS
					if (!System.IO.Directory.Exists(strBackupPath))
						if (!System.IO.Directory.Exists(System.Web.HttpContext.Current.Server.MapPath(strBackupPath)))
							return false;
											
					// APPEND THE BACKUP FILENAME ONTO THE DIRECTORY STRUCTURE
					strBackupPath += strDatabase + DateTime.Now.AddHours(2).ToString("_yyyyMMdd-HHmm") + ".bak";
					strBackupPath =  System.Web.HttpContext.Current.Server.MapPath(strBackupPath);

					// IF THE BACKUP FILE ALREADY EXISTS, DELETE IT.
					//	OTHERWISE, THE SQL COMMAND WILL JUST APPEND TO THE FILE
					if (File.Exists(strBackupPath))
						System.IO.File.Delete(strBackupPath);

					// PERFORM THE BACKUP
					if (_DALType == DALtypes.MSSQL)
						return SQLBackupDatabase(strDatabase, strBackupPath);
				} catch (Exception ex) { 
//				this.ReportError("ClsDAL", "BackupDatabase", strBackupPath, ex);
				}

				return false;
			}
*/		
		#endregion
		
	#endregion

}
