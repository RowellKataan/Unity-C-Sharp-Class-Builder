// ===========================================================================================================
//
// Class/Library: DatabaseManager Inspector/Editor
//        Author: Michael Marzilli   ( http://www.linkedin.com/in/michaelmarzilli )
//       Created: Mar 26, 2016
//	
// VERS 1.0.000 : Mar 26, 2016 : Original File Created. Released for Unity 3D.
//			1.1.001 : Sep 27, 2017 : Added functionality to support SQLite Databases.
//															 Added functionality to support MySQL  Databases.
//
// ===========================================================================================================

using UnityEngine;
using UnityEditor;
using UnityEditor.SceneManagement;
using System.Collections;

[CustomEditor(typeof(DatabaseManager))]
public class DatabaseManagerEditor : Editor
{

	private string strDec = "";

	public override void OnInspectorGUI()
	{
		DatabaseManager myTarget = null;
		try { myTarget = (DatabaseManager)target; } catch { }

		if (myTarget != null)
		{
				GUI.changed = false;
								
				EditorStyles.label.fontStyle	= FontStyle.Bold;
				EditorGUILayout.LabelField("DATABASE CONFIGURATION");
				EditorStyles.label.fontStyle	= FontStyle.Normal;
				myTarget.DatabaseType					= (ClsDAL.DBtypes) EditorGUILayout.EnumPopup("Database Type", myTarget.DatabaseType);

				EditorGUILayout.Separator();
				EditorGUILayout.Space();
				switch (myTarget.DatabaseType)
				{
					case ClsDAL.DBtypes.MSSQL:
					case ClsDAL.DBtypes.MYSQL:
							myTarget.DBtextFile						= (TextAsset)EditorGUILayout.ObjectField("Override File", myTarget.DBtextFile, typeof(TextAsset), true);
							myTarget.DBserver							= EditorGUILayout.TextField("Database Server",		myTarget.DBserver);
							myTarget.DBport								= EditorGUILayout.IntField(	"Database Port",			myTarget.DBport);
							myTarget.DBdatabase						= EditorGUILayout.TextField("Database Name",			myTarget.DBdatabase);
							if (myTarget.DatabaseType == ClsDAL.DBtypes.MSSQL)
									myTarget.DBuseWindowsAccount	= EditorGUILayout.Toggle("Use Windows Account",		myTarget.DBuseWindowsAccount);
							else
									myTarget.DBuseWindowsAccount	= false;
							if (myTarget.DBport < 1)
									myTarget.DBport = (myTarget.DatabaseType == ClsDAL.DBtypes.MSSQL) ? 1433 : 3306;
							if (!myTarget.DBuseWindowsAccount)
							{ 
								myTarget.DBuser							= EditorGUILayout.TextField("Username",						myTarget.DBuser);
								myTarget.DBpassword					= EditorGUILayout.PasswordField("Password",				myTarget.DBpassword);		// PasswordField
							}
							break;
					case ClsDAL.DBtypes.SQLITE:
							myTarget.SQLiteDBfileLocation	= EditorGUILayout.TextField("Database File",			myTarget.SQLiteDBfileLocation);
							break;
				}

				EditorGUILayout.Separator();
				EditorGUILayout.Space();
				myTarget.KeepConnectionOpen		= EditorGUILayout.Toggle("Keep Connection Open",	myTarget.KeepConnectionOpen);
				myTarget.ClientsCanUse				= EditorGUILayout.Toggle(		"Client Can Use DB",	myTarget.ClientsCanUse);


				if (myTarget.DatabaseType == ClsDAL.DBtypes.MSSQL)
				{
					EditorGUILayout.Separator();
					EditorGUILayout.Space();
					EditorGUILayout.TextField("Encrypted Text", strDec, GUILayout.MaxHeight(75));
					if (GUILayout.Button("Show Encryption Text"))
					{
						strDec  = "";
						strDec += "Server: "   + myTarget.DBserver + "," + myTarget.DBport.ToString() + "\n";
						strDec += "Database: " + myTarget.DBdatabase + "\n";
						strDec += "Username: " + Crypto.Encrypt(myTarget.DBuser) + "\n";
						strDec += "Password: " + Crypto.Encrypt(myTarget.DBpassword) + "\n";
					}
				}

				if (Application.isPlaying)
				{
					EditorGUILayout.Separator();
					EditorGUILayout.Space();
					EditorStyles.label.fontStyle	= FontStyle.Bold;
					EditorGUILayout.LabelField("DATABASE STATUS");
					EditorStyles.label.fontStyle	= FontStyle.Normal;
					EditorGUILayout.Toggle(		 "Is Online?",			myTarget.IsConnectedCheck);

					EditorGUILayout.LabelField("Last Query: ",		myTarget.DAL.LastQueryTime.ToString("0.000000") + " seconds");
					EditorGUILayout.LabelField("Average Query: ", myTarget.DAL.AverageQueryTime.ToString("0.000000") + " seconds");
					EditorGUILayout.Space();

					EditorStyles.label.wordWrap				= true;
					EditorStyles.label.stretchWidth		= false;
					EditorStyles.label.stretchHeight	= true;
					EditorGUILayout.LabelField("Last Command",	(myTarget.DAL.SQLqueries == "")	? "(None)" : myTarget.DAL.SQLqueries);
					EditorGUILayout.LabelField("Error Message", (myTarget.DAL.Errors == "")			? "(None)" : myTarget.DAL.Errors);
					EditorStyles.label.wordWrap				= false;
					EditorStyles.label.stretchHeight	= false;
					if (GUILayout.Button("Reset SQL Errors"))
						myTarget.DAL.ResetErrors();
				}

				if (GUI.changed)
				{
					EditorUtility.SetDirty(myTarget);
					EditorSceneManager.MarkSceneDirty(EditorSceneManager.GetActiveScene());
				}
		}
	}
}
