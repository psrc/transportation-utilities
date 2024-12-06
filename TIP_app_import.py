#-------------------------------------------------------------------------------
import os, time

######################################
# CONFIGURATION
# Change the following four variables depending on the file being imported.
workingfile = 'BELL-101.txt' # The name of the txt file to import
workingdir = 'X:/Trans/TIP/Amendments/Amend/CY2023/23-02/Import' # the folder that the file sits in
amendment = 'some-amendment-name' # The amendment that the project will be part of, e.g. "25-02"
proj_id = 'test'  # The project id.
#######################################

db_name = 'RTIPDataSQL'
db = db_name + '.dbo'

opentags = ['tblReviewEnviro',
            'tblReviewFinancial',
            'tblReviewProjectCost',
            'tblReviewProjMTP',
            'tblReviewRTIP',
            'tblReviewSecondaryImpType',
            'tblReviewSpecial96_98',
            'tblReviewNonmotorized',
            'tblReviewPhaseInfo']


tagtables = {'tblReviewEnviro':'tblStageEnviro',
            'tblReviewFinancial':'tblStageFinancial',
            'tblReviewProjectCost':'tblStageProjectCost',
            'tblReviewProjMTP':'tblStageProjMTP',
            'tblReviewRTIP':'tblStageRTIP',
           'tblReviewSecondaryImpType':'tblStageSecondaryImpType',
            'tblReviewSpecial96_98':'tblStageSpecial96_98',
            'tblReviewNonmotorized':'tblStageNonmotorized',
            'tblReviewPhaseInfo':'tblStagePhaseInfo'}


class interchangefile:
    def __init__(self, workingdir, filename):
        self.filename= filename
        self.file = workingdir + '/' + filename
        self.opentags = opentags
        #self.closetags= closetags

    def split_to_sections(self):
        # Split data into temp files
        try:
            infile = open(self.file,'r', encoding='utf-8')
            inlines = infile.readlines()
            tagindex = 0
            for line in inlines:
                opentag = '<' + opentags[tagindex] + '>'
                closetag = '</' + opentags[tagindex] + '>'
                rawtag = opentags[tagindex]

                line = line.replace(' UTC ', ' ')
                line = line.replace(' UTC\n', '')
                if rawtag == 'tblReviewProjectCost':
                    line = line.replace('val:true', 'val:1')
                    line = line.replace('val:false', 'val:0')
                if line == opentag + '\n':
                    outfile = open(workingdir + '/' + rawtag + '.tmp', 'w' , encoding='utf-8')
                elif line == closetag + '\n':
                    outfile.close()
                    tagindex = tagindex + 1
                else:
                    print (line)
                    outfile.write(line)
            infile.close()

        except Exception as e:
            print('error in split_to_sections.')
            print(e.args[0])
            print(infile)
            raise


    def cleanup_files(self):
        for tagname in self.opentags:
            #command = 'del ' + tagname + '.tmp'
            command =  'move ' + tagname  + '.tmp '  + 'imported/' + tagname + '.tmp'
            print (command)
            os.system(command)
        #command = 'move ' + self.file + ' ' + workingdir + '/imported/' + self.filename
        command =  'move ' + self.filename + ' '  + 'imported/' + self.filename
        #print command
        #os.system(command)

    def importdata(self):
        sproc_command = 'SQLCMD -S AWS-PROD-SQL\Sockeye -E -d "' + db_name + '" -Q "EXEC tipsp_DeleteFromStagingTables"'
        os.system(sproc_command)
        for tag in opentags:
            dbtable = tagtables[tag]
            mytagtable = workingdir + '/' + tag + '.tmp'
            bcp_command = 'BCP ' + db + '.' + dbtable + ' in ' + mytagtable + ' -c -t val: -S AWS-PROD-SQL\Sockeye -T'
            #bi_command = 'BULK INSERT ' + db + '.' + dbtable + ' FROM ' + tag + '.tmp WITH DATAFILETYPE = "char", FIELDTERMINATOR="val:"'
            #full_bi_command = 'SQLCMD -S SQL2016\DSAPROD -E -d "RTIPDataSQL_importtest" -Q ' + bi_command
            print (bcp_command)
            #print full_bi_command
            os.system(bcp_command)
            #os.system(full_bi_command)
        sproc_command = 'SQLCMD -S AWS-PROD-SQL\Sockeye -E -d "' + db_name + '" -Q "EXEC tipsp_StageToReview \'' + proj_id + '\', \'' + amendment + '\'"'
        #print sproc_command
        os.system(sproc_command)


ifile = interchangefile(workingdir, workingfile)
ifile.split_to_sections()
time.sleep(2)
ifile.importdata()
#ifile.cleanup_files()
print ('finished importing ' + ifile.filename)

