from dbConnection import *
from configparser import ConfigParser
from getopt       import getopt

def main():
  # Config file
  config = ConfigParser()
  config.read("conf.cfg")
  # Command-line args
  argumentList = sys.argv[1:]
  options      = "h"
  long_options = ["Help"]
  try:
    arguments,values = getopt(argumentList,options,long_options)
  except Exception as err:
    print(err)
    print(f"Unknown option")
    exit(-1)
  # Handle options
  for currentArgument,currentValue in arguments:
    if currentArgument in ("-h", "--Help"):
      print("Usage: python3 correctingDgwNode.py [-h] \n\nh | Help : Display help and usage\n")
      exit(0)
  # Connect to DB
  print("Connecting to DGW")
  dgw  = DBConnection(
    config.get("DEFAULT","DGW_IP"),
    config.get("DEFAULT","DGW_PORT"),
    config.get("DEFAULT","DGW_DB_NAME"),
    config.get("DEFAULT","DGW_DB_USER"),
    config.get("DEFAULT","DGW_DB_PWD")
  )
  dgw.connect()
  print(f"Connecting to {config.get('DEFAULT','NODE_NAME')}")
  node = DBConnection(
    config.get("DEFAULT","NODE_IP"),
    config.get("DEFAULT","NODE_PORT"),
    config.get("DEFAULT","NODE_DB_NAME"),
    config.get("DEFAULT","NODE_DB_USER"),
    config.get("DEFAULT","NODE_DB_PWD")
  )
  node.connect()
  print("--------")
  insertCommands = []
  updateCommands = []
  deleteCommands = []
  deleteCommands2 = []
  node.cursor.execute("SELECT id FROM station;")
  nodeStationIds = node.cursor.fetchall()
  for nodeStationId in nodeStationIds:
    node.cursor.execute("SELECT * FROM rinex_file WHERE id_station = %s",(nodeStationId[0],))
    nodeFiles = node.cursor.fetchall()
    dgw.cursor.execute("SELECT * FROM rinex_file WHERE id_station = %s",(nodeStationId[0],))
    dgwFiles = dgw.cursor.fetchall()
    for file in nodeFiles:
      if file[13] > 0 and not isIn(file,dgwFiles):
        insertCommands.append(f"INSERT INTO rinex_file VALUES({','.join([item for item in file])});")
        print("File added for insert")
      elif file[13] > 0 and isIn(file,dgwFiles):
        creationDate = 'NULL' if not file[8] else "'" + str(file[8]) + "'"
        publishedDate = 'NULL' if not file[9] else "'" + str(file[9]) + "'"
        revisionDate = 'NULL' if not file[10] else "'" + str(file[10]) + "'"
        updateCommands.append(f"UPDATE rinex_file SET id_station = {file[2]}, id_data_center_structure = {file[3]}, file_size = {file[4]}, id_file_type = {file[5]}, relative_path = '{file[6]}', reference_date = '{file[7]}', creation_date = {creationDate}, published_date = {publishedDate}, revision_date = {revisionDate}, md5checksum = '{file[11]}', md5uncompressed = '{file[12]}' WHERE md5checksum = '{file[11]}';")
        print("File added for update")
      elif file[13] <= 0 and isIn(file,dgwFiles):
        deleteCommands.append(f"DELETE FROM rinex_file WHERE id = {file[0]};")
        print("File added for delete")
    for file in dgwFiles:
      if not isIn(file,nodeFiles):
        deleteCommands2.append(f"DELETE FROM rinex_file WHERE id = {file[0]};")
        print("File added for delete")
  print("--------")
  print(f"Finished processing files. {len(insertCommands)} files to be inserted, {len(updateCommands)} files to be updated and {len(deleteCommands)} + {len(deleteCommands2)} ({len(deleteCommands) + len(deleteCommands2)}) files to be deleted for a total of {len(insertCommands)+len(updateCommands)+len(deleteCommands)+len(deleteCommands2)} files to be changed.")
  with open("insertCommands.sql","w") as f:
    f.write("BEGIN;\n")
    for command in insertCommands:
      f.write(command)
      f.write("\n")
    f.write("COMMIT;")
  print("insertCommands.sql created")
  with open("updateCommands.sql","w") as f:
    f.write("BEGIN;\n")
    for command in updateCommands:
      f.write(command)
      f.write("\n")
    f.write("COMMIT;")
  print("updateCommands.sql created")
  with open("deleteCommands.sql","w") as f:
    f.write("BEGIN;\n")
    for command in deleteCommands:
      f.write(command)
      f.write("\n")
    for command in deleteCommands2:
      f.write(command)
      f.write("\n")
    f.write("COMMIT;")
  print("deleteCommands.sql created")
  print("--------")
  
def isIn(file,dgwFiles):
  for dgwFile in dgwFiles:
    if file[11] == dgwFile[11]:
      return True
  return False
            
if __name__ == "__main__":
    main()