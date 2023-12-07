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
  print("Connecting to NOA")
  noa  = DBConnection(
    config.get("DEFAULT","NOA_IP"),
    config.get("DEFAULT","NOA_PORT"),
    config.get("DEFAULT","NOA_DB_NAME"),
    config.get("DEFAULT","NOA_DB_USER"),
    config.get("DEFAULT","NOA_DB_PWD")
  )
  noa.connect()
  print("--------")
  insertCommands = []
  updateCommands = []
  deleteCommands = []
  noa.cursor.execute("SELECT id FROM station;")
  noaStationIds = noa.cursor.fetchall()
  for noaStationId in noaStationIds:
    noa.cursor.execute("SELECT * FROM rinex_file WHERE id_station = %s",(noaStationId[0],))
    noaFiles = noa.cursor.fetchall()
    dgw.cursor.execute("SELECT * FROM rinex_file WHERE id_station = %s",(noaStationId[0],))
    dgwFiles = dgw.cursor.fetchall()
    for file in noaFiles:
      if file[13] > 0 and file not in dgwFiles:
        areFilesDifferentButEqual = [filesDifferentButEqual(file,dgwFile) for dgwFile in dgwFiles]
        if all(fileDifferent[0] == -1 for fileDifferent in areFilesDifferentButEqual):
          insertCommands.append(f"INSERT INTO rinex_file VALUES({','.join([item for item in file])});")
          print("File added for insert")
        for fileDifferent in areFilesDifferentButEqual:
          if fileDifferent[0] == 1:
            updateCommands.append(f"UPDATE rinex_file SET id = {fileDifferent[2][0]} id_station = {fileDifferent[2][2]}, id_data_center_structure = {fileDifferent[2][3]}, file_size = {fileDifferent[2][4]}, id_file_type = {fileDifferent[2][5]}, relative_path = {fileDifferent[2][6]}, reference_date = {fileDifferent[2][7]}, creation_date = {fileDifferent[2][8]}, published_date = {fileDifferent[2][9]}, revision_date = {fileDifferent[2][10]}, md5checksum = {fileDifferent[2][11]}, md5uncompressed = {fileDifferent[2][12]}, status = {fileDifferent[2][13]};")
            print("File added for update")
      elif file[13] <= 0 and file in dgwFiles:
        deleteCommands.append(f"DELETE FROM rinex_file WHERE id = {file[0]};")
        print("File added for delete")
  print("--------")
  print(f"Finished processing files. {len(insertCommands)} files to be inserted, {len(updateCommands)} files to be updated and {len(deleteCommands)} files to be deleted for a total of {len(insertCommands)+len(updateCommands)+len(deleteCommands)} files to be changed.")
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
    f.write("COMMIT;")
  print("deleteCommands.sql created")
  print("--------")
  
def filesDifferentButEqual(file1,file2):
  if file1[1] == file2[1]: #filename
    if any([file1[i] != file2[i] for i in range(2,13)]): #all other fields, besides id and status
      return [1,file1,file2]
    else:
      return [0,"",""]
  else:
    return [-1,"",""]
            
if __name__ == "__main__":
    main()