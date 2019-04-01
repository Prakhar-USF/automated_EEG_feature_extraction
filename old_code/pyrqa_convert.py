
### This library does some stuff to try to convert pyrqa into a python 3 library

import os
import subprocess
import shutil
import site


#So I think that this should work:
#1. Create a directory
#2. Remove the first line if as belows <= this is probably what is failing
#3. run 2to3 and 
#4. (smomestime optiona, depending on version) in the settings.py file added "." to the import lines
#5. In the code from Bill, I need to chagne the following: file_start = pd.to_datetime(metarow['Start_time'], format='%m/%d/%Y %H:%M:%S %p'))
#6. I removed the following dependenices from Bill's code: matploblib and eegtools


def convertPY3( dirname ):

	targetDir = dirname + '/pyrqa3/'
	sourceDir = dirname + '/pyrqa/'
		
	if os.path.exists(sourceDir) == False:
		print("It doesn't seem like pyrqa is installed at %s" % sourceDir)
		raise Exception('Source Error')
	try:
		### This shold be where you are going to install pyrqa3. on my dumb system I have to put it in 2 places hence two lines. 
		### Most systems should only have a single line (Remove the second)
		shutil.rmtree(targetDir)
	except:
		pass

	### This should copy from pyrqa to a new directory pyrqa3.
	shutil.copytree( sourceDir, targetDir)

	### For each file in pyrqa3 remove the first row if it of the form

	for root, dirs, files in os.walk(targetDir):
		for file in files:
			if file.endswith('.py'):

				### if the file is python file, copy to temporary spot, delete the orig and then do a line-by-line copy
				try:
					os.remove('delme.tmp')
				except:
					pass

				shutil.copyfile(os.path.join(root, file), 'delme.tmp')

				os.remove( os.path.join(root, file) )
				
				from_file = open('delme.tmp', 'r') 
				to_file = open(os.path.join(root, file), mode="w")
				

				lines = from_file.readlines()


				for line in lines:

					line_to_write = line
					if line.strip() == '#!/usr/bin/python':
						line_to_write = '\n'
					elif line.strip().startswith('from pyrqa.'):
						line_to_write = line.replace('pyrqa', 'pyrqa3', 1)
				
					to_file.write(line_to_write)

				to_file.close()

				subprocess.run(["2to3","-w", os.path.join(root, file), "/dev/null"], stdout=subprocess.PIPE)

	os.remove('delme.tmp')

	### the following shoould be removed unless you, like me, need to install it in two locations

if __name__ == "__main__":


	### Logic -- provide a directory here. If this is empty it will ask if want to use the default site-packages location
	### Note that it can be a list of multiple locations i fyou want to do more than one. 

	site_packakges_dir_list = []
	if site_packakges_dir_list == []:
		site_packakges_dir_list =  site.getsitepackages()

	res = input("Would you like to target: \n %s \n (Y/N)" % '\n'.join(site_packakges_dir_list))

	
	if res.upper() == 'Y':
		for dir in site_packakges_dir_list:

			convertPY3( dir )

## EOF ##
