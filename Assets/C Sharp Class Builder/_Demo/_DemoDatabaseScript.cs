// ===========================================================================================================
//
// Class/Library: Demo Database Script -- Used to test connection and interaction with MSSQL, MySQL and SQLite Databases.
//        Author: Michael Marzilli   ( http://www.linkedin.com/in/michaelmarzilli )
//       Created: Sep 25, 2007
//	
// VERS 1.0.000 : Sep 25, 2007 : Original File Created.
//
// ===========================================================================================================

using UnityEngine;
using UnityEngine.UI;
using System;
using System.Data;

public class _DemoDatabaseScript : MonoBehaviour 
{

	#region "PRIVATE VARIABLES"

		[SerializeField,HideInInspector]
		private string					_strServer		= "";
		[SerializeField,HideInInspector]
		private string					_strDatabase	= "";
		[SerializeField,HideInInspector]
		private string					_strUsername	= "";
		[SerializeField,HideInInspector]
		private string					_strPassword	= "";
		[SerializeField,HideInInspector]
		private string					_strFileLoc		= "";
		[SerializeField,HideInInspector]
		private string					_strTableName	= "";
		[SerializeField,HideInInspector]
		private int							_intPort			= 1433;
		[SerializeField,HideInInspector]
		private int							_intType			= 0;

		private string					_strOutput		= "";

		private	DatabaseManager	_dbm	= null;

	#endregion

	#region "PRIVATE PROPERTIES"

		private DatabaseManager	DB
		{
			get
			{
				if (_dbm == null)
						_dbm = DatabaseManager.Instance;
				return _dbm;
			}
		}

	#endregion

	#region "PUBLIC EDITOR PROPERTIES"

		public	InputField			ServerInp;
		public	InputField			PortInp;
		public	InputField			DatabaseInp;
		public	InputField			UsernameInp;
		public	InputField			PasswordInp;
		public	InputField			FileLocInp;
		public	InputField			TableNameInp;
		public	Dropdown				TypeDDL;
		public	Text						OutputText;
		public	GameObject			UpdateButton;
		public	InputField			IDinp;

	#endregion

	#region "PUBLIC PROPERTIES"

		public	string					Server
		{
			get
			{
				return _strServer;
			}
			set
			{
				_strServer = value.Trim();
				if (ServerInp != null)
						ServerInp.text = _strServer;
			}
		}
		public	int							Port
		{
			get
			{
				return _intPort;
			}
			set
			{
				_intPort = value;
				if (PortInp != null)
						PortInp.text = _intPort.ToString();
			}
		}
		public	string					Database
		{
			get
			{
				return _strDatabase;
			}
			set
			{
				_strDatabase = value.Trim();
				if (DatabaseInp != null)
						DatabaseInp.text = _strDatabase;
			}
		}
		public	string					Username
		{
			get
			{
				return _strUsername;
			}
			set
			{
				_strUsername = value.Trim();
				if (UsernameInp != null)
						UsernameInp.text = _strUsername;
			}
		}
		public	string					Password
		{
			get
			{
				return _strPassword;
			}
			set
			{
				_strPassword = value.Trim();
				if (PasswordInp != null)
						PasswordInp.text = _strPassword;
			}
		}
		public	string					FileLocation
		{
			get
			{
				return _strFileLoc;
			}
			set
			{
				_strFileLoc = value.Trim();
				if (FileLocInp != null)
						FileLocInp.text = _strFileLoc;
			}
		}
		public	string					TableName
		{
			get
			{
				return _strTableName;
			}
			set
			{
				_strTableName = value.Trim();
				if (TableNameInp != null)
						TableNameInp.text = _strTableName;
			}
		}
		public	string					Output
		{
			get
			{
				return _strOutput;
			}
			set
			{
				_strOutput = value.Trim();
				if (OutputText != null)
						OutputText.text = _strOutput;
			}
		}
		public	ClsDAL.DBtypes	DatabaseType
		{
			get
			{
				if (TypeDDL != null)
						_intType = TypeDDL.value;
				return (ClsDAL.DBtypes) _intType;
			}
			set
			{
				_intType = (int) value;
				if (TypeDDL != null)
						TypeDDL.value = _intType;
			}
		}

	#endregion

	#region "PRIVATE FUNCTIONS"

		private void						Start()
		{
			DeserializeData();
			TypeDDL.value = _intType;
			UpdateDatabaseSettings();
			ButtonUpdateDatabaseOnClick();
		}

		private void						UpdateDatabaseSettings()
		{
			// SET THE DROPDOWN LIST TO THE APPROPRIATE DATABASE TYPE
			DatabaseType = (ClsDAL.DBtypes) TypeDDL.value;

			// SHOW OR HIDE THE APPROPRIATE FIELDS BASED ON DATABASE TYPE
			switch ((ClsDAL.DBtypes) _intType)
			{
				case ClsDAL.DBtypes.MSSQL:
				case ClsDAL.DBtypes.MYSQL:
					ServerInp.gameObject.SetActive(true);
					PortInp.gameObject.SetActive(true);
					DatabaseInp.gameObject.SetActive(true);
					UsernameInp.gameObject.SetActive(true);
					PasswordInp.gameObject.SetActive(true);
					FileLocInp.gameObject.SetActive(false);
					break;
				case ClsDAL.DBtypes.SQLITE:
					ServerInp.gameObject.SetActive(false);
					PortInp.gameObject.SetActive(false);
					DatabaseInp.gameObject.SetActive(false);
					UsernameInp.gameObject.SetActive(false);
					PasswordInp.gameObject.SetActive(false);
					FileLocInp.gameObject.SetActive(true);
					break;
			}

			// PRE-FILL FIELDS WITH APPROPRIATE DATA
			ServerInp.text		= _strServer;
			PortInp.text			= _intPort.ToString();
			DatabaseInp.text	= _strDatabase;
			UsernameInp.text	= _strUsername;
			PasswordInp.text	= _strPassword;
			FileLocInp.text		= _strFileLoc;
			TableNameInp.text	= _strTableName;

			// UPDATE DATABASEMANAGER AND DAL
			DB.DatabaseType		= DatabaseType;
			DB.DBserver				= Server;
			DB.DBport					= Port;
			DB.DBdatabase			= Database;
			DB.DBuser					= Username;
			DB.DBpassword			= Password;
			DB.SQLiteDBfileLocation	= FileLocation;

			// STORE THE SETTINGS IN A PLAYER PREF
			SerializeData();
		}
		private void						UpdateOutput(string strOut)
		{
			if (OutputText != null)
					OutputText.text = strOut + "\n\n" + ((DB.DBerrors != "") ? DB.DBerrors : "");
		}

		private void						SerializeData()
		{
			string strSer = "";
			strSer += _intType.ToString() + "|";
			strSer += _strServer		+ "|";
			strSer += _intPort.ToString() + "|";
			strSer += _strDatabase	+ "|";
			strSer += _strUsername	+ "|";
			strSer += _strPassword	+ "|";
			strSer += _strFileLoc		+ "|";
			strSer += _strTableName;
			PlayerPrefs.SetString("CBT.Demo.Settings", strSer);
		}
		private void						DeserializeData()
		{
			string strSer = PlayerPrefs.GetString("CBT.Demo.Settings");
			string[] s = strSer.Split('|');

			_intType			= Util.ConvertToInt(s[0]);
			_strServer		= s[1];
			_intPort			= Util.ConvertToInt(s[2]);
			_strDatabase	= s[3];
			_strUsername	= s[4];
			_strPassword	= s[5];
			_strFileLoc		= s[6];
			_strTableName	= s[7];
		}

	#endregion

	#region "PUBLIC EVENTS"

		#region "DROP DOWN LIST EVENTS"

			public	void						TypeDDLonChange()
			{
				switch (TypeDDL.value)
				{
					case 0:		// MSSQL
						_intPort = 1433;
						break;
					case 1:		// MYSQL
						_intPort = 3306;
						break;
				}
				UpdateDatabaseSettings();
				if (UpdateButton != null)
						UpdateButton.SetActive(true);
			}

		#endregion

		#region "INPUT FIELD CHANGE EVENTS"

			public	void						ServerOnChange()
			{
				Server = ServerInp.text;
				UpdateDatabaseSettings();
				if (UpdateButton != null)
						UpdateButton.SetActive(true);
			}
			public	void						PortOnChange()
			{
				Port = Util.ConvertToInt(PortInp.text);
				UpdateDatabaseSettings();
				if (UpdateButton != null)
						UpdateButton.SetActive(true);
			}
			public	void						DatabaseOnChange()
			{
				Database = DatabaseInp.text;
				UpdateDatabaseSettings();
				if (UpdateButton != null)
						UpdateButton.SetActive(true);
			}
			public	void						UsernameOnChange()
			{
				Username = UsernameInp.text;
				UpdateDatabaseSettings();
				if (UpdateButton != null)
						UpdateButton.SetActive(true);
			}
			public	void						PasswordOnChange()
			{
				Password = PasswordInp.text;
				UpdateDatabaseSettings();
				if (UpdateButton != null)
						UpdateButton.SetActive(true);
			}
			public	void						FileLocationOnChange()
			{
				FileLocation = FileLocInp.text;
				UpdateDatabaseSettings();
				if (UpdateButton != null)
						UpdateButton.SetActive(true);
			}
			public	void						TableNameOnChange()
			{
				TableName = TableNameInp.text;
				UpdateDatabaseSettings();
			}

		#endregion

		#region "BUTTON EVENTS"

			public	void						ButtonUpdateDatabaseOnClick()
			{
				DB.InitializeDAL();
				if (UpdateButton != null)
						UpdateButton.SetActive(false);
			}

		#endregion

	#region "--- HERE'S WHERE THE SQL MAGIC HAPPENS ---"
	#endregion

		#region "DIRECT SQL BUTTONS"

			public	void						ButtonBasicSelectOnClick()
			{
				// QUERY THE DATABASE -- THE RESULTS ARE PLACED INTO A DATATABLE
				string strSQL = "SELECT * FROM " + _strTableName.ToLower();
				DB.ClearParams();
				DataTable dt = DB.GetSQLSelectDataTable(strSQL);

				// DISPLAY THE DATA
				if (dt != null && dt.Rows.Count > 0)
				{
					string strOut = "\n\nTable: " + _strTableName + ", " + dt.Rows.Count.ToString() + " Rows selected\n\n       ";
					for (int h = 0; h < dt.Columns.Count; h++)
						strOut += dt.Columns[h].ColumnName + ", ";
					strOut += "\n";
					for (int i = 0; i < dt.Rows.Count; i++)
					{
						strOut += "Row #" + (i + 1).ToString() + ": ";
						for (int c = 0; c < dt.Columns.Count; c++)
						{
							strOut += dt.Rows[i][c].ToString() + ", ";
						}
						strOut += "\n";
					}
					UpdateOutput(strOut + "\n\n" + strSQL);
				} else
					UpdateOutput("\n\nNo Records Selected for Table: " + _strTableName + "\n\n" + strSQL);
			}
			public	void						ButtonSelectFirstRecordNameOnClick()
			{
				// QUERY THE DATABASE -- THE RESULT IS RETURNED AS A STRING
				string strSQL = "SELECT Name FROM " + _strTableName + " WHERE ID=1";
				DB.ClearParams();
				string strOut = DB.GetSQLSelectString(strSQL);

				// DISPLAY THE DATA
				if (strOut != "")
				{
					strOut = "\n\nTable: " + _strTableName + "\n\n" + "Output: " + strOut + "\n";
					UpdateOutput(strOut + "\n\n" + strSQL);
				} else
					UpdateOutput("\n\nNo Records Selected for Table: " + _strTableName + "\n\n" + strSQL);
			}
			public	void						ButtonUpdateFirstRecordAgeOnClick()
			{
				string strOut = "";
				string strSQL = "UPDATE " + _strTableName + " SET Age=Age+1 WHERE ID=1";

				// UPDATE THE DATABASE
				// DISPLAY THE RESULTS
				DB.ClearParams();
				if (DB.DoSQLUpdateDelete(strSQL))
				{
					strSQL = "SELECT Name FROM " + _strTableName + " WHERE ID=1";
					DataTable dt = DB.DAL.GetSQLSelectDataTable("SELECT * FROM " + _strTableName + " WHERE ID=1");
					if (dt != null && dt.Rows.Count > 0)
					{
						strOut = "\n\nTable: " + _strTableName + " - " + dt.Rows[0]["NAME"].ToString() + " is now " + dt.Rows[0]["AGE"].ToString() + " years old.\n";
						UpdateOutput(strOut + "\n\n" + strSQL);
					}
				} else
					UpdateOutput("\n\nNo Records Updated for Table: " + _strTableName + "\n\n" + strSQL);
			}

		#endregion

		#region "STORED PROCEDURE BUTTONS"
			
			public	void						ButtonSPgetByIDOnClick()
			{
				// DATA INTEGRITY CHECKS
				if (IDinp == null)
					return;
				if (DatabaseType == ClsDAL.DBtypes.SQLITE)
				{
					UpdateOutput("\n\nSQLite is unable to execute stored procedures.");
					Debug.LogError("SQLite is unable to execute stored procedures.");
					return;
				}
				int intID = Util.ConvertToInt(IDinp.text);
				if (intID < 1)
				{
					UpdateOutput("\n\nID should be greater than zero.");
					Debug.LogError("ID should be greater than zero.");
					return;
				}

				// PERFORM THE STORED PROCEDURE -- RESULTS ARE RETURNED AS A DATATABLE
				DB.ClearParams();
				DB.AddParam("SEARCHID", intID);
				DataTable dt = DB.GetSPDataTable("GetTestByID");

				// DISPLAY THE DATA
				if (dt != null && dt.Rows.Count > 0)
				{
					string strOut = "\n\n" + dt.Rows.Count.ToString() + " Rows selected\n\n       ";
					for (int h = 0; h < dt.Columns.Count; h++)
						strOut += dt.Columns[h].ColumnName + ", ";
					strOut += "\n";
					for (int i = 0; i < dt.Rows.Count; i++)
					{
						strOut += "Row #" + (i + 1).ToString() + ": ";
						for (int c = 0; c < dt.Columns.Count; c++)
						{
							strOut += dt.Rows[i][c].ToString() + ", ";
						}
						strOut += "\n";
					}
					UpdateOutput(strOut + "\n\n");
				} else
					UpdateOutput("\n\nNo Records Selected for Table: " + _strTableName + "\n\n");
			}
			public	void						ButtonSPupdateByIDOnClick()
			{
				// DATA INTEGRITY CHECKS
				if (IDinp == null)
					return;
				if (DatabaseType == ClsDAL.DBtypes.SQLITE)
				{
					UpdateOutput("\n\nSQLite is unable to execute stored procedures.");
					Debug.LogError("SQLite is unable to execute stored procedures.");
					return;
				}
				int intID = Util.ConvertToInt(IDinp.text);
				if (intID < 1)
				{
					UpdateOutput("\n\nID should be greater than zero.");
					Debug.LogError("ID should be greater than zero.");
					return;
				}

				// PERFORM THE UPDATE STORED PROCEDURE
				DB.ClearParams();
				DB.AddParam("SEARCHID", intID);
				DB.ExecuteSP("UpdateAgeByID");

				// DISPLAY THE RECORD
				ButtonSPgetByIDOnClick();
			}

		#endregion

	#endregion

}
