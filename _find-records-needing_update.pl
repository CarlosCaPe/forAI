###########################################################################################
#                                                                                         #
# Name: find-records-to-load.pl                                                            #
#                                                                                         #
# Description:                                                                            #
#    
#                                                                                         #
#                                                                                         #
#  	Logic:                                                                                 #
#	This Perl script appears to be used for processing and manipulating data from CSV files. Here's a high-level summary of what it does:
#
#	*Initialization: 	The script begins by declaring a bunch of variables and constants, including arrays, hashes, and strings. 
#	 					Among these variables is a hash called %COURSE_CERT, which is initialized with a set of key-value pairs where 
#						each key represents a specific course or certification and each value is "X".
#
#	*User Input: 		The script then asks the user to provide contact information and the names of various files to be processed, 
#						including a "Salesforce export CSV file" and "previous file". The filenames are taken without extension, 
#						then .csv is appended to form the complete filename.
#
#	*Data Processing: 	Once the files are specified, the script begins processing them. It appears to perform a series of operations, including:
#						
#						**Reading the Salesforce file (Read_SFFILE function which is not visible in the provided script)
#						**Reading the contact information file (Read_Contact function which is not visible in the provided script)
#						**Reading the previous file (Read_PFILE function which is not visible in the provided script)
#						**Writing headers to some files (HEADERS function which is not visible in the provided script)
#						**Performing some sort of comparison operation (Difference function which is not visible in the provided script)
#						**Retrieving student IDs (Get_STUDENT_Safe_ID function which is not visible in the provided script)
#						**Processing single matches (Process_Single_Match function which is not visible in the provided script)
#						**Writing to a load file (Write_Load_File function which is not visible in the provided script)


###########################################################################################

use strict;
use DateTime;


############################################################################################
#   Constants and Variables required by the program                                        #
############################################################################################

our @Elements;
our @DElements;
our @DATE_Elements;
our $Line;
our $Line1;
our %Entry=();
our %PFILE=();
our $tmpFile;
our $INPUTFILE;
our $ContactFILE;
our $OUTPUTFILE;
our $Row=0;
our $Answer;
our $FMT_DATE;
our $DATE_COL;
our $Date;
our $MM;
our $YY;
our $DD;
our $index;
our $key;
our $INPUTPFILE;
our %DEntry=();
our %Diff=();
our $ERRCFile;
our @EMAILElements;
our %CS_1stID=();
our %CS_2ndID=();
our %CS_3rdID=();
our $ERRORFILE;
our %SMATCH=();
our %SMATCH_LINE=();
our $SFFILE;
our %SF_Data=();
our %LOAD_FILE=();
our $SMERROR_FILE;
our $AlreadyLoaded;
our $full_key;
our $DIFF_FILE;
our $CHECKFILE;
our $Done;
our $KEY;
our $Multi_match;
our $Single_match;
our %COURSE_CERT=();
our $NO_CERT_MATCH;
our $NO_SPOT_FOUND;
our $ID;
our $KEY1;
our $KEY2;
our $KEY3;
our $KEY_good;
our $KEY_MULTI;
our $NOSPOT_PRINT=0;
our $NOCERTM_PRINT=0;
our $SMERROR_PRINT=0;
our $EFILE_PRINT=0;
our $MFILE_PRINT=0;
############################################################################################
#   Main					                                           #
############################################################################################
$COURSE_CERT{"BT-1000|RBT"}="X";
$COURSE_CERT{"BT-1000|CMAA"}="X";
$COURSE_CERT{"BT-1100|RBT"}="X";
$COURSE_CERT{"DA-3000|RDA"}="X";
$COURSE_CERT{"EA-1000|ITF+"}="X";
$COURSE_CERT{"HI-1000|CMAA"}="X";
$COURSE_CERT{"HI-1000|MCBC"}="X";
$COURSE_CERT{"HI-1000P|CMAA"}="X";
$COURSE_CERT{"HI-1000P|MCBC"}="X";
$COURSE_CERT{"HI-1100|MCBC"}="X";
$COURSE_CERT{"HI-1100P|MCBC"}="X";
$COURSE_CERT{"HI-1200|CMAA"}="X";
$COURSE_CERT{"HI-2000|CPhT"}="X";
$COURSE_CERT{"HI-2000|CMAA"}="X";
$COURSE_CERT{"HI-2100|CPhT"}="X";
$COURSE_CERT{"HI-2100P|CPhT"}="X";
$COURSE_CERT{"HI-3000|CMAA"}="X";
$COURSE_CERT{"HI-3000|CEHRS"}="X";
$COURSE_CERT{"HI-3100|CEHRS"}="X";
$COURSE_CERT{"HI-4000|CEHRS"}="X";
$COURSE_CERT{"HI-4000|MCBC"}="X";
$COURSE_CERT{"HI-5000|MCBC"}="X";
$COURSE_CERT{"HI-5000|CPCT"}="X";
$COURSE_CERT{"HI-5000|CCS-P"}="X";
$COURSE_CERT{"HI-5100|CPC-A"}="X";
$COURSE_CERT{"HI-5100|CCS-P"}="X";
$COURSE_CERT{"HI-6000|CCMA"}="X";
$COURSE_CERT{"HI-6000|CMAA"}="X";
$COURSE_CERT{"SBC-HI6000|CCMA"}="X";
$COURSE_CERT{"SBC-HI6000|CMAA"}="X";
$COURSE_CERT{"HI-6000A|CCMA"}="X";
$COURSE_CERT{"HI-6000A|CMAA"}="X";
$COURSE_CERT{"HI-6000P|CCMA"}="X";
$COURSE_CERT{"HI-6000P|CMAA"}="X";
$COURSE_CERT{"HI-6010|CCMA"}="X";
$COURSE_CERT{"HI-6100|CPT"}="X";
$COURSE_CERT{"HI-6100P|CPT"}="X";
$COURSE_CERT{"HI-6200|CET"}="X";
$COURSE_CERT{"HI-6200P|CET"}="X";
$COURSE_CERT{"HI-6300|CPT"}="X";
$COURSE_CERT{"HI-6300|CET"}="X";
$COURSE_CERT{"HI-6400|AMSP"}="X";
$COURSE_CERT{"HI-6500|AMSP"}="X";
$COURSE_CERT{"HI-6500|CMAA"}="X";
$COURSE_CERT{"HI-6600|CMLA"}="X";
$COURSE_CERT{"HI-6600|CPT"}="X";
$COURSE_CERT{"HI-7000|CPCT"}="X";
$COURSE_CERT{"HI-7000|CPCT-A"}="X";
$COURSE_CERT{"HI-7000|CPT"}="X";
$COURSE_CERT{"HI-7000|CET"}="X";
$COURSE_CERT{"HI-7000P|CPCT"}="X";
$COURSE_CERT{"HI-7000P|CPCT-A"}="X";
$COURSE_CERT{"HI-7000P|CPT"}="X";
$COURSE_CERT{"HI-7000P|CET"}="X";
$COURSE_CERT{"HI-8000|CHUC"}="X";
$COURSE_CERT{"HI-8000|CEHRS"}="X";
$COURSE_CERT{"HI-9000|CMAA"}="X";
$COURSE_CERT{"HI-9000|MCBC"}="X";
$COURSE_CERT{"HI-9000|CEHRS"}="X";
$COURSE_CERT{"HI-9000|CPhT"}="X";
$COURSE_CERT{"HI-9500|CMAA"}="X";
$COURSE_CERT{"HI-9500|MCBC"}="X";
$COURSE_CERT{"HI-9500|CEHRS"}="X";
$COURSE_CERT{"HI-9600|CCMA"}="X";
$COURSE_CERT{"HI-9600|CMAA"}="X";
$COURSE_CERT{"HI-9600|CPT"}="X";
$COURSE_CERT{"HI-9700|CCMA"}="X";
$COURSE_CERT{"HI-9700|CMAA"}="X";
$COURSE_CERT{"HI-9700|CPT"}="X";
$COURSE_CERT{"HI-9700|CET"}="X";
$COURSE_CERT{"HI-9800|AMSP"}="X";
$COURSE_CERT{"HI-9800|CMAA"}="X";
$COURSE_CERT{"HI-9800|CCMA"}="X";
$COURSE_CERT{"IT-1000|ITF+"}="X";
$COURSE_CERT{"IT-1000|A+1001"}="X";
$COURSE_CERT{"IT-1000|A+1002"}="X";
$COURSE_CERT{"IT-1100|ITF+"}="X";
$COURSE_CERT{"IT-2000|A+1001"}="X";
$COURSE_CERT{"IT-2000|A+1002"}="X";
$COURSE_CERT{"IT-2000|Net+"}="X";
$COURSE_CERT{"IT-2100|A+1001"}="X";
$COURSE_CERT{"IT-2100|A+1002"}="X";
$COURSE_CERT{"IT-2200|Net+"}="X";
$COURSE_CERT{"IT-3000|A+1001"}="X";
$COURSE_CERT{"IT-3000|A+1002"}="X";
$COURSE_CERT{"IT-3000|Net+"}="X";
$COURSE_CERT{"IT-3000|CEHRS"}="X";
$COURSE_CERT{"IT-4100|CCNA"}="X";
$COURSE_CERT{"IT-5000|Sec+"}="X";
$COURSE_CERT{"IT-5000|Net+"}="X";
$COURSE_CERT{"IT-5100|Sec+"}="X";
$COURSE_CERT{"IT-6100|PenTest+"}="X";
$COURSE_CERT{"IT-6200|CySA+"}="X";
$COURSE_CERT{"IT-7000|CSAA"}="X";
$COURSE_CERT{"IT-7000|Cloud+"}="X";
$COURSE_CERT{"IT-7100|Cloud+"}="X";
$COURSE_CERT{"IT-7200|CSAA"}="X";
$COURSE_CERT{"MS-7000|MOS"}="X";
$COURSE_CERT{"PM-6100|CAPM"}="X";
$COURSE_CERT{"PT-3000|PTTC"}="X";
$COURSE_CERT{"PT-3000|CMAA"}="X";
$COURSE_CERT{"RX-3000|CPhT"}="X";
$COURSE_CERT{"RX-3000A|CPhT"}="X";
$COURSE_CERT{"SP-3000|CRCST"}="X";
$COURSE_CERT{"SP-3000P|CRCST"}="X";
$COURSE_CERT{"ST-3000|CRCST"}="X";
$COURSE_CERT{"ST-3000P|CRCST"}="X";
$COURSE_CERT{"ST-9000|TS-C"}="X";
$COURSE_CERT{"ST-9000|CRCST"}="X";
$COURSE_CERT{"VA-3000|AVA"}="X";
$COURSE_CERT{"VA-4000|CMAA"}="X";


#Temp ones
#$COURSE_CERT{"HI-1000|CBCS"}="X";
#$COURSE_CERT{"HI-1100|CBCS"}="X";
#$COURSE_CERT{"HI-9500|CBCS"}="X";
#$COURSE_CERT{"HI-4000|CBCS"}="X";
#$COURSE_CERT{"HI-1000P|CBCS"}="X";
#$COURSE_CERT{"HI-9000|CBCS"}="X";
#$COURSE_CERT{"HI-5000|CBCS"}="X";
#$COURSE_CERT{"HI-1100P|CBCS"}="X";
#$COURSE_CERT{"HI-6000E|CCMA"}="X";
#$COURSE_CERT{"HI-6000E|CMAA"}="X";
#$COURSE_CERT{"DA - 4000|CMAA"}="X";
#$COURSE_CERT{"DA-4000|CMAA"}="X";
############################################################################################
#   Main Menu				                                           #
############################################################################################
print "\n\n";
Get_Contact_info();
print "\n\n";
print " Input the SF export CSV file to process  ( no extension)\n";
$tmpFile=<STDIN>;
chomp($tmpFile);
$SFFILE=$tmpFile . ".csv";
print "\n\n";
print " Input the file to process( no extension)\n";
$tmpFile=<STDIN>;
chomp($tmpFile);
$INPUTFILE=$tmpFile . ".csv";
$OUTPUTFILE="Need_to_be_loaded_" .$INPUTFILE;
$ERRORFILE="MISSING_STUDENT_MATCH" .$INPUTFILE;
$SMERROR_FILE="Single_MATCH_ISSUE" . $INPUTFILE;
$CHECKFILE="Check_load_file" . $INPUTFILE;
$DIFF_FILE="Difference_n_input" . $INPUTFILE;
$AlreadyLoaded="AlreadyLoaded" . $INPUTFILE;
$Multi_match="Multi_Match" . $INPUTFILE;
$Single_match="Single_Match" . $INPUTFILE;
$NO_CERT_MATCH="NO_CERT_MATCH" . $INPUTFILE;
$NO_SPOT_FOUND="NO_SPOT_FOUND" . $INPUTFILE;
print "\n\n";
print " Input the Previous file( no extension)\n";
$tmpFile=<STDIN>;
chomp($tmpFile);
$INPUTPFILE=$tmpFile . ".csv";

 

############################################################################################
#   Start Processing			                                           #
############################################################################################
open(SFFILE,"<$SFFILE") or die "File $SFFILE does not exist";
Read_SFFILE();

open(ConFILE,"<$ContactFILE") or die "$ContactFILE can not be open";	
#open(CERROR,">$ERRCFile") or die "$ERRCFile can not be open";	

Read_Contact();

open(PFILE,"<$INPUTPFILE") or die "File $INPUTPFILE does not exist";
Read_PFILE();

open(IFILE,"<$INPUTFILE") or die "File $INPUTFILE does not exist";
open(OFILE,">$OUTPUTFILE") or die "File $OUTPUTFILE can not be open";
#open(SFILE,">$Single_match") or die "File $Single_match can not be open";

open(CFILE,">$CHECKFILE") or die "File $CHECKFILE can not be open";
#open(AFILE,">$AlreadyLoaded") or die "File $AlreadyLoaded can not be open";
#open(DFILE,">$DIFF_FILE") or die "File $DIFF_FILE can not be open";


HEADERS();
# The HEADERS subroutine in this Perl script is writing column headers to four different output files: OFILE, CFILE, AFILE, and SFILE.
# Each of the print statements in the HEADERS subroutine is printing a string of comma-separated values (CSV) to a particular file. Each CSV string corresponds to the column headers for the data that will be written to each file.
# In more detail:
# *print OFILE "Case Safe ID,First Name,Last Name,Email,... prints the headers for the OFILE. These headers represent different fields like Case Safe ID, First Name, Last Name, Email, Certification 1 Registered Program, Certification 1 Registration Date, and so on.
# *print CFILE "Source,Case Safe ID,First Name,Last Name,Email,... does the same for the CFILE. The headers are similar to those in OFILE but there's an additional Source field at the beginning.
# *print AFILE "First Name,Last Name,Student Email,Institution,... prints the headers for the AFILE. These headers represent fields related to students, institutions, exams, and certifications.
# *print SFILE "Case Safe ID,First Name,Last Name,Student Email,Institution,... prints the headers for the SFILE. These are similar to the AFILE headers, but with an additional Case Safe ID field at the beginning.
# *The EOL at the end of each print statement stands for End Of Line. It's likely defined elsewhere in the script, probably as a newline character (\n), or carriage return and newline (\r\n) depending on the system the script is intended to run on. This is used to ensure that each header line is correctly terminated and the following data starts on a new line.


Difference();
# The Difference subroutine is essentially comparing lines from an input file (IFILE) with the entries in a hash (PFILE). Here's a step-by-step explanation of its operation:
# *It initializes a counter $Row to keep track of the current line number being processed.
# *It reads the input file IFILE line by line in a while loop.
# *For each line, it removes the trailing newline character with chomp.
# *It increments the $Row counter.
# *It checks if the first three characters of the line are not ",,,". If they are not, it proceeds with the following steps:
# 	-It splits the line by commas and stores the resulting elements in an array @Elements.
# 	-It forms a new string $Line1 by concatenating the first 15 elements of @Elements with commas.
# 	-If it is processing the first line ($Row eq 1), it writes $Line1 to the output file DFILE.
# 	-For lines other than the first, it checks if $Line1 exists as a key in the hash %PFILE. If it does, it does nothing (represented by the ;).
# 	-If $Line1 does not exist in %PFILE, it writes $Line1 followed by ",EOL" to DFILE and also adds $Line1 to another hash %Diff with "X" as the value.
# The purpose of this subroutine seems to be comparing the contents of IFILE against a set of known values in %PFILE, and writing any differences to an output file DFILE while also keeping track of the differences in %Diff. Note that the output lines are appended with ",EOL" to indicate the end of line.
# However, please note that this code contains some potentially confusing elements. For example, ",EOL" is added to $Line1 while forming it and then again when printing it to DFILE if it's not found in %PFILE. Also, a hash entry in %Diff is made with "X" as the value, but it's unclear how this hash is used later.


Get_STUDENT_Safe_ID();
# The Get_STUDENT_Safe_ID subroutine in Perl seems to be doing the following:
# -It sorts the keys of the %Diff hash and then iterates over these sorted keys.
# -For each key (referred to as $Line), it splits the line into elements by using commas as separators.
# -It takes the third element from the split line (indexes in Perl start from 0, so $Elements[2] refers to the third element), converts it to lowercase, and stores it in the $key variable.
# -It checks if this $key exists in any of the three hashes, $CS_1stID, $CS_2ndID, or $CS_3rdID.
# -If $key exists in any of these hashes, it then checks if $key exists in $CS_1stID but not in $CS_2ndID or $CS_3rdID. If this condition is true, it calls the Process_One_Contact subroutine, which presumably processes data related to a single contact.
# -If the condition in step 5 is not met (i.e., $key exists in more than one hash), it calls the Process_Multi_Contacts subroutine, which presumably processes data related to multiple contacts.
# -If $key does not exist in any of the three hashes, it checks if $EFILE_PRINT is equal to 0. If it is, it calls the EFILE_HEADERS subroutine, which may print headers to the EFILE.
# -It then prints the original $Line to EFILE, followed by ,EOL\n, which presumably adds an end-of-line marker and a newline character.

Process_Single_Match();
# The Process_Single_Match subroutine appears to be processing single matches from a hash (associative array) named %SMATCH, and possibly recording or updating data in another hash named %SF_Data. This processing is done on the basis of certain conditions and checks.
# This is a high-level interpretation of what the subroutine is doing:
# -It first iterates over each key in the %SMATCH hash. Each key appears to be a string containing multiple fields separated by a pipe (|) character, which are split into an array @Elements. The first element of this array becomes the new $key.
# -The subroutine then performs a series of checks and operations for each $key. It seems to be handling data related to some sort of course or exam system, where each key in %SMATCH represents a unique identifier for a student, and %SF_Data stores detailed information about the student's progress or status in various exams (E1 through E7).
# -For each exam (E1 through E7), the subroutine checks whether the student has registered (REGDATE field is empty), and if not, it calls a function Create_LOADFILE_Ex() (where x is the exam number) to presumably create or update some data.
# -If the student has registered for the exam (REGDATE field is not empty), the subroutine then checks whether the student's exam details in %SMATCH match those in %SF_Data. If all details match, it prints a message to a file AFILE saying that the data is already loaded in SF (presumably Salesforce). If the REGDATE matches but other details do not, it again calls Create_LOADFILE_Ex().


Write_Load_File();
# The Write_Load_File() subroutine in your Perl script is performing the following operations:
# -It loops over each key in the LOAD_FILE hash, which appears to be a hash of hashes, where each key maps to a hash that stores data about an individual.
# -For each key in the LOAD_FILE hash, it writes a series of fields to two separate files. These fields include details such as CSID, FName, LName, Email, and various event details (E1Cert, E1REGDATE, E1APPDATE, etc.).
# -The data is written to two different files, OFILE and CFILE, in a specific format. The output is a comma-separated line for each key in the LOAD_FILE hash.
# -In the case of OFILE, only the data from the LOAD_FILE hash is written.
# -In the case of CFILE, after writing the data from the LOAD_FILE hash, it also writes a similar set of data from the SF_Data hash for the same key.
# -The data written into CFILE starts with identifiers (INPUT and SF respectively) to differentiate between the data from LOAD_FILE and SF_Data.
# -The EOL at the end of each print statement is presumably a predefined variable which represents the end of a line (possibly "\n" for a newline).
# In summary, this subroutine exports the data in LOAD_FILE and SF_Data hashes to two different files in a structured, comma-separated format.


exit;


sub Process_Single_Match{

	foreach $full_key (sort keys %SMATCH ){	
		@Elements=split(/\|/,$full_key);		
		$key=$Elements[0];
						
		$Done="n";
# add check for program
		$KEY=$SF_Data{$key}->{PCODE} . "|" . $SMATCH{$full_key}->{Cert};
		$ID=substr($SF_Data{$key}->{StudentID},0,1);
	   if (exists $COURSE_CERT{$KEY} || $ID eq "K" ){
		#print "$full_key,$ID\n";
#exam 1
		if ( $SF_Data{$key}->{E1Cert} eq "" && exists $SF_Data{$key}){
			if ( $SF_Data{$key}->{E1REGDATE} eq "" ){
				Create_LOADFILE_E1();
				$Done="y";
			}
		}
		if ( $SMATCH{$full_key}->{Cert} eq $SF_Data{$key}->{E1Cert}  && $Done eq "n" ){
			if ( $SF_Data{$key}->{E1REGDATE} eq "" && $SF_Data{$key}->{E1SchDate} eq "" && $SF_Data{$key}->{E1EXDATE} eq "" && $SF_Data{$key}->{E1Score} eq "" && $SF_Data{$key}->{E1PF} eq ""){
		
				Create_LOADFILE_E1();
				$Done="y";
			}

			if ( exists $SF_Data{$key} &&  $SF_Data{$key}->{E1REGDATE} ne "" && $Done eq "n" && $SMATCH{$full_key}->{Cert} eq $SF_Data{$key}->{E1Cert} ){

		
				if ( $SF_Data{$key}->{E1REGDATE} eq $SMATCH{$full_key}->{Regdate} ){
					if ( $SF_Data{$key}->{E1APPDATE} eq $SMATCH{$full_key}->{Appdate} && $SF_Data{$key}->{E1SchDate} eq $SMATCH{$full_key}->{Plandate} && $SF_Data{$key}->{E1EXDATE} eq $SMATCH{$full_key}->{Examdate} && $SF_Data{$key}->{E1Score} eq $SMATCH{$full_key}->{ExamScore} && $SF_Data{$key}->{E1PF} eq $SMATCH{$full_key}->{ExamPF}){
						print AFILE "$full_key  already loaded in SF\n";
						$Done="y";
					}else{
						if ( $SF_Data{$key}->{E1REGDATE} eq $SMATCH{$full_key}->{Regdate} ){
							Create_LOADFILE_E1();
							$Done="y";
						}
					}
				}
			}
		}
#exam 2
		if ( $SF_Data{$key}->{E2Cert} eq "" && exists $SF_Data{$key} && $Done eq "n"){
			if ( $SF_Data{$key}->{E2REGDATE} eq "" ){
				Create_LOADFILE_E2();
				$Done="y";
			}
		}
		if ( $SMATCH{$full_key}->{Cert} eq $SF_Data{$key}->{E2Cert}  && $Done eq "n" ){
			if (  $SF_Data{$key}->{E2REGDATE} eq "" && $SF_Data{$key}->{E2SchDate} eq "" && $SF_Data{$key}->{E2EXDATE} eq "" && $SF_Data{$key}->{E2Score} eq "" && $SF_Data{$key}->{E2PF} eq ""){
				
				Create_LOADFILE_E2();
				$Done="y";
			}

			if ( exists $SF_Data{$key} &&  $SF_Data{$key}->{E2REGDATE} ne "" && $Done eq "n" && $SMATCH{$full_key}->{Cert} eq $SF_Data{$key}->{E2Cert} ){

			
				if ( $SF_Data{$key}->{E2REGDATE} eq $SMATCH{$full_key}->{Regdate} ){
					if (  $SF_Data{$key}->{E2APPDATE} eq $SMATCH{$full_key}->{Appdate} && $SF_Data{$key}->{E12SchDate} eq $SMATCH{$full_key}->{Plandate} && $SF_Data{$key}->{E2EXDATE} eq $SMATCH{$full_key}->{Examdate} && $SF_Data{$key}->{E2Score} eq $SMATCH{$full_key}->{ExamScore} && $SF_Data{$key}->{E2PF} eq $SMATCH{$full_key}->{ExamPF}){
						print AFILE "$full_key  already loaded in SF\n";
						$Done="y";
					}else{
						if ( $SF_Data{$key}->{E2REGDATE} eq $SMATCH{$full_key}->{Regdate} ){
							Create_LOADFILE_E2();
							$Done="y";
						}
					}
				}
			}
		}
#exam 3
		if ( $SF_Data{$key}->{E3Cert} eq "" && exists $SF_Data{$key} && $Done eq "n"){
			if ( $SF_Data{$key}->{E3REGDATE} eq "" ){
				Create_LOADFILE_E3();
				$Done="y";
			}
		}
		if ( $SMATCH{$full_key}->{Cert} eq $SF_Data{$key}->{E3Cert}  && $Done eq "n" ){
			if ( $SF_Data{$key}->{E3REGDATE} eq "" && $SF_Data{$key}->{E3SchDate} eq "" && $SF_Data{$key}->{E3EXDATE} eq "" && $SF_Data{$key}->{E3Score} eq "" && $SF_Data{$key}->{E3PF} eq ""){
				
				Create_LOADFILE_E3();
				$Done="y";
			}

			if ( exists $SF_Data{$key} &&  $SF_Data{$key}->{E3REGDATE} ne "" && $Done eq "n" && $SMATCH{$full_key}->{Cert} eq $SF_Data{$key}->{E3Cert} ){

			
				if ( $SF_Data{$key}->{E3REGDATE} eq $SMATCH{$full_key}->{Regdate} ){
					if (  $SF_Data{$key}->{E3APPDATE} eq $SMATCH{$full_key}->{Appdate} && $SF_Data{$key}->{E3SchDate} eq $SMATCH{$full_key}->{Plandate} && $SF_Data{$key}->{E3EXDATE} eq $SMATCH{$full_key}->{Examdate} && $SF_Data{$key}->{E3Score} eq $SMATCH{$full_key}->{ExamScore} && $SF_Data{$key}->{E3PF} eq $SMATCH{$full_key}->{ExamPF}){
						print AFILE "$full_key  already loaded in SF\n";
						$Done="y";
					}else{
						if ( $SF_Data{$key}->{E3REGDATE} eq $SMATCH{$full_key}->{Regdate} ){
							Create_LOADFILE_E3();
							$Done="y";
						}
					}
				}
			}
		}
#exam 4
		if ( $SF_Data{$key}->{E4Cert} eq "" && exists $SF_Data{$key} && $Done eq "n"){
			if ( $SF_Data{$key}->{E4REGDATE} eq "" ){
				Create_LOADFILE_E4();
				$Done="y";
			}
		}
		if ( $SMATCH{$full_key}->{Cert} eq $SF_Data{$key}->{E4Cert}  && $Done eq "n" ){
			if ( $SF_Data{$key}->{E4REGDATE} eq "" && $SF_Data{$key}->{E4SchDate} eq "" && $SF_Data{$key}->{E4EXDATE} eq "" && $SF_Data{$key}->{E4Score} eq "" && $SF_Data{$key}->{E4PF} eq ""){
				
				Create_LOADFILE_E4();
				$Done="y";
			}

			if ( exists $SF_Data{$key} &&  $SF_Data{$key}->{E4REGDATE} ne "" && $Done eq "n" && $SMATCH{$full_key}->{Cert} eq $SF_Data{$key}->{E4Cert} ){

			
				if ( $SF_Data{$key}->{E4REGDATE} eq $SMATCH{$full_key}->{Regdate} ){
					if ( $SF_Data{$key}->{E4EXDATE} eq $SMATCH{$full_key}->{Examdate} && $SF_Data{$key}->{E4Score} eq $SMATCH{$full_key}->{ExamScore} && $SF_Data{$key}->{E4PF} eq $SMATCH{$full_key}->{ExamPF}){
						print AFILE "$full_key  already loaded in SF\n";
						$Done="y";
					}else{
						if ( $SF_Data{$key}->{E4REGDATE} eq $SMATCH{$full_key}->{Regdate} ){
							Create_LOADFILE_E4();
							$Done="y";
						}
					}
				}
			}
		}

#exam 5
		if ( $SF_Data{$key}->{E5Cert} eq "" && exists $SF_Data{$key} && $Done eq "n"){
			if ( $SF_Data{$key}->{E5REGDATE} eq "" ){
				Create_LOADFILE_E5();
				$Done="y";
			}
		}
		if ( $SMATCH{$full_key}->{Cert} eq $SF_Data{$key}->{E5Cert}  && $Done eq "n" ){
			if ( $SF_Data{$key}->{E5REGDATE} eq "" && $SF_Data{$key}->{E5SchDate} eq "" && $SF_Data{$key}->{E5EXDATE} eq "" && $SF_Data{$key}->{E5Score} eq "" && $SF_Data{$key}->{E5PF} eq ""){
				
				Create_LOADFILE_E5();
				$Done="y";
			}

			if ( exists $SF_Data{$key} &&  $SF_Data{$key}->{E5REGDATE} ne "" && $Done eq "n" && $SMATCH{$full_key}->{Cert} eq $SF_Data{$key}->{E5Cert} ){

			
				if ( $SF_Data{$key}->{E5REGDATE} eq $SMATCH{$full_key}->{Regdate} ){
					if ( $SF_Data{$key}->{E5EXDATE} eq $SMATCH{$full_key}->{Examdate} && $SF_Data{$key}->{E5Score} eq $SMATCH{$full_key}->{ExamScore} && $SF_Data{$key}->{E5PF} eq $SMATCH{$full_key}->{ExamPF}){
						print AFILE "$full_key  already loaded in SF\n";
						$Done="y";
					}else{
						if ( $SF_Data{$key}->{E5REGDATE} eq $SMATCH{$full_key}->{Regdate} ){
							Create_LOADFILE_E5();
							$Done="y";
						}
					}
				}
			}
		}
#exam 6
		if ( $SF_Data{$key}->{E6Cert} eq "" && exists $SF_Data{$key} && $Done eq "n"){
			if ( $SF_Data{$key}->{E6REGDATE} eq "" ){
				Create_LOADFILE_E6();
				$Done="y";
			}
		}
		if ( $SMATCH{$full_key}->{Cert} eq $SF_Data{$key}->{E6Cert}  && $Done eq "n" ){
			if ( $SF_Data{$key}->{E6REGDATE} eq "" && $SF_Data{$key}->{E6SchDate} eq "" && $SF_Data{$key}->{E6EXDATE} eq "" && $SF_Data{$key}->{E6Score} eq "" && $SF_Data{$key}->{E6PF} eq ""){
				
				Create_LOADFILE_E6();
				$Done="y";
			}

			if ( exists $SF_Data{$key} &&  $SF_Data{$key}->{E6REGDATE} ne "" && $Done eq "n" && $SMATCH{$full_key}->{Cert} eq $SF_Data{$key}->{E6Cert} ){

			
				if ( $SF_Data{$key}->{E6REGDATE} eq $SMATCH{$full_key}->{Regdate} ){
					if (  $SF_Data{$key}->{E6APPDATE} eq $SMATCH{$full_key}->{Appdate} && $SF_Data{$key}->{E6SchDate} eq $SMATCH{$full_key}->{Plandate} && $SF_Data{$key}->{E6EXDATE} eq $SMATCH{$full_key}->{Examdate} && $SF_Data{$key}->{E6Score} eq $SMATCH{$full_key}->{ExamScore} && $SF_Data{$key}->{E6PF} eq $SMATCH{$full_key}->{ExamPF}){
						print AFILE "$full_key  already loaded in SF\n";
						$Done="y";
					}else{
						if ( $SF_Data{$key}->{E6REGDATE} eq $SMATCH{$full_key}->{Regdate} ){
							Create_LOADFILE_E6();
							$Done="y";
						}
					}
				}
			}
		}
#exam 7
		if ( $SF_Data{$key}->{E7Cert} eq "" && exists $SF_Data{$key} && $Done eq "n"){
			if ( $SF_Data{$key}->{E7REGDATE} eq "" ){
				Create_LOADFILE_E7();
				$Done="y";
			}
		}
		if ( $SMATCH{$full_key}->{Cert} eq $SF_Data{$key}->{E7Cert}  && $Done eq "n" ){
			if ( $SF_Data{$key}->{E7REGDATE} eq "" && $SF_Data{$key}->{E7SchDate} eq "" && $SF_Data{$key}->{E7EXDATE} eq "" && $SF_Data{$key}->{E7Score} eq "" && $SF_Data{$key}->{E7PF} eq ""){
				
				Create_LOADFILE_E7();
				$Done="y";
			}

			if ( exists $SF_Data{$key} &&  $SF_Data{$key}->{E7REGDATE} ne "" && $Done eq "n" && $SMATCH{$full_key}->{Cert} eq $SF_Data{$key}->{E7Cert} ){

			
				if ( $SF_Data{$key}->{E7REGDATE} eq $SMATCH{$full_key}->{Regdate} ){
					if (  $SF_Data{$key}->{E7APPDATE} eq $SMATCH{$full_key}->{Appdate} && $SF_Data{$key}->{E7SchDate} eq $SMATCH{$full_key}->{Plandate} && $SF_Data{$key}->{E7EXDATE} eq $SMATCH{$full_key}->{Examdate} && $SF_Data{$key}->{E7Score} eq $SMATCH{$full_key}->{ExamScore} && $SF_Data{$key}->{E7PF} eq $SMATCH{$full_key}->{ExamPF}){
						print AFILE "$full_key  already loaded in SF\n";
						$Done="y";
					}else{
						if ( $SF_Data{$key}->{E7REGDATE} eq $SMATCH{$full_key}->{Regdate} ){
							Create_LOADFILE_E7();
							$Done="y";
						}
					}
				}
			}
		}
#exam 8
		if ( $SF_Data{$key}->{E8Cert} eq "" && exists $SF_Data{$key} && $Done eq "n"){
			if ( $SF_Data{$key}->{E8REGDATE} eq "" ){
				Create_LOADFILE_E8();
				$Done="y";
			}
		}
		if ( $SMATCH{$full_key}->{Cert} eq $SF_Data{$key}->{E8Cert}  && $Done eq "n" ){
			if ( $SF_Data{$key}->{E8REGDATE} eq "" && $SF_Data{$key}->{E8SchDate} eq "" && $SF_Data{$key}->{E8EXDATE} eq "" && $SF_Data{$key}->{E8Score} eq "" && $SF_Data{$key}->{E8PF} eq ""){
				
				Create_LOADFILE_E8();
				$Done="y";
			}

			if ( exists $SF_Data{$key} &&  $SF_Data{$key}->{E8REGDATE} ne "" && $Done eq "n" && $SMATCH{$full_key}->{Cert} eq $SF_Data{$key}->{E8Cert} ){

			
				if ( $SF_Data{$key}->{E8REGDATE} eq $SMATCH{$full_key}->{Regdate} ){
					if ( $SF_Data{$key}->{E8APPDATE} eq $SMATCH{$full_key}->{Appdate} && $SF_Data{$key}->{E8SchDate} eq $SMATCH{$full_key}->{Plandate} &&  $SF_Data{$key}->{E8EXDATE} eq $SMATCH{$full_key}->{Examdate} && $SF_Data{$key}->{E8Score} eq $SMATCH{$full_key}->{ExamScore} && $SF_Data{$key}->{E8PF} eq $SMATCH{$full_key}->{ExamPF}){
						print AFILE "$full_key  already loaded in SF\n";
						$Done="y";
					}else{
						if ( $SF_Data{$key}->{E8REGDATE} eq $SMATCH{$full_key}->{Regdate} ){
							Create_LOADFILE_E8();
							$Done="y";
						}
					}
				}
			}
		}
#No Spot
		if (  $Done eq "n"){
			if ( $NOSPOT_PRINT eq 0 ){
				NOSPOT_HEADERS();
			}
			print NOSPOT "$SMATCH_LINE{$full_key}\n";
		}

           }else{
		if ( $NOCERTM_PRINT eq 0 ){
			NOCERTM_HEADERS();
		}

		print NOCERTM "$SMATCH_LINE{$full_key}\n";
	   }


	}
						
}

sub Create_LOADFILE_E1{
	$LOAD_FILE{$key}->{CSID}=$key;
	$LOAD_FILE{$key}->{Email}=$SMATCH{$full_key}->{Email};
	$LOAD_FILE{$key}->{FName}=$SMATCH{$full_key}->{FName};
	$LOAD_FILE{$key}->{LName}=$SMATCH{$full_key}->{LName};
	$LOAD_FILE{$key}->{E1REGDATE}=$SMATCH{$full_key}->{Regdate};
	$LOAD_FILE{$key}->{E1SchDate}=$SMATCH{$full_key}->{Plandate};
	$LOAD_FILE{$key}->{E1EXDATE}=$SMATCH{$full_key}->{Examdate};
	$LOAD_FILE{$key}->{E1Score}=$SMATCH{$full_key}->{ExamScore};
	$LOAD_FILE{$key}->{E1PF}=$SMATCH{$full_key}->{ExamPF};
	$LOAD_FILE{$key}->{E1Cert}=$SMATCH{$full_key}->{Cert};

	$LOAD_FILE{$key}->{E1APPDATE}=$SMATCH{$full_key}->{Appdate};


}
sub Create_LOADFILE_E2{
	$LOAD_FILE{$key}->{CSID}=$key;
	$LOAD_FILE{$key}->{Email}=$SMATCH{$full_key}->{Email};
	$LOAD_FILE{$key}->{FName}=$SMATCH{$full_key}->{FName};
	$LOAD_FILE{$key}->{LName}=$SMATCH{$full_key}->{LName};
	$LOAD_FILE{$key}->{E2REGDATE}=$SMATCH{$full_key}->{Regdate};
	$LOAD_FILE{$key}->{E2SchDate}=$SMATCH{$full_key}->{Plandate};
	$LOAD_FILE{$key}->{E2EXDATE}=$SMATCH{$full_key}->{Examdate};
	$LOAD_FILE{$key}->{E2Score}=$SMATCH{$full_key}->{ExamScore};
	$LOAD_FILE{$key}->{E2PF}=$SMATCH{$full_key}->{ExamPF};
	$LOAD_FILE{$key}->{E2Cert}=$SMATCH{$full_key}->{Cert};
	$LOAD_FILE{$key}->{E2APPDATE}=$SMATCH{$full_key}->{Appdate};


}
sub Create_LOADFILE_E3{
	$LOAD_FILE{$key}->{CSID}=$key;
	$LOAD_FILE{$key}->{Email}=$SMATCH{$full_key}->{Email};
	$LOAD_FILE{$key}->{FName}=$SMATCH{$full_key}->{FName};
	$LOAD_FILE{$key}->{LName}=$SMATCH{$full_key}->{LName};
	$LOAD_FILE{$key}->{E3REGDATE}=$SMATCH{$full_key}->{Regdate};
	$LOAD_FILE{$key}->{E3SchDate}=$SMATCH{$full_key}->{Plandate};
	$LOAD_FILE{$key}->{E3EXDATE}=$SMATCH{$full_key}->{Examdate};
	$LOAD_FILE{$key}->{E3Score}=$SMATCH{$full_key}->{ExamScore};
	$LOAD_FILE{$key}->{E3PF}=$SMATCH{$full_key}->{ExamPF};
	$LOAD_FILE{$key}->{E3Cert}=$SMATCH{$full_key}->{Cert};
	$LOAD_FILE{$key}->{E3APPDATE}=$SMATCH{$full_key}->{Appdate};


}
sub Create_LOADFILE_E4{
	$LOAD_FILE{$key}->{CSID}=$key;
	$LOAD_FILE{$key}->{Email}=$SMATCH{$full_key}->{Email};
	$LOAD_FILE{$key}->{FName}=$SMATCH{$full_key}->{FName};
	$LOAD_FILE{$key}->{LName}=$SMATCH{$full_key}->{LName};
	$LOAD_FILE{$key}->{E4REGDATE}=$SMATCH{$full_key}->{Regdate};
	$LOAD_FILE{$key}->{E4SchDate}=$SMATCH{$full_key}->{Plandate};
	$LOAD_FILE{$key}->{E4EXDATE}=$SMATCH{$full_key}->{Examdate};
	$LOAD_FILE{$key}->{E4Score}=$SMATCH{$full_key}->{ExamScore};
	$LOAD_FILE{$key}->{E4PF}=$SMATCH{$full_key}->{ExamPF};
	$LOAD_FILE{$key}->{E4Cert}=$SMATCH{$full_key}->{Cert};
	$LOAD_FILE{$key}->{E4APPDATE}=$SMATCH{$full_key}->{Appdate};


}
sub Create_LOADFILE_E5{
	$LOAD_FILE{$key}->{CSID}=$key;
	$LOAD_FILE{$key}->{Email}=$SMATCH{$full_key}->{Email};
	$LOAD_FILE{$key}->{FName}=$SMATCH{$full_key}->{FName};
	$LOAD_FILE{$key}->{LName}=$SMATCH{$full_key}->{LName};
	$LOAD_FILE{$key}->{E5REGDATE}=$SMATCH{$full_key}->{Regdate};
	$LOAD_FILE{$key}->{E5SchDate}=$SMATCH{$full_key}->{Plandate};
	$LOAD_FILE{$key}->{E5EXDATE}=$SMATCH{$full_key}->{Examdate};
	$LOAD_FILE{$key}->{E5Score}=$SMATCH{$full_key}->{ExamScore};
	$LOAD_FILE{$key}->{E5PF}=$SMATCH{$full_key}->{ExamPF};
	$LOAD_FILE{$key}->{E5Cert}=$SMATCH{$full_key}->{Cert};
	$LOAD_FILE{$key}->{E5APPDATE}=$SMATCH{$full_key}->{Appdate};


}
sub Create_LOADFILE_E6{
	$LOAD_FILE{$key}->{CSID}=$key;
	$LOAD_FILE{$key}->{Email}=$SMATCH{$full_key}->{Email};
	$LOAD_FILE{$key}->{FName}=$SMATCH{$full_key}->{FName};
	$LOAD_FILE{$key}->{LName}=$SMATCH{$full_key}->{LName};
	$LOAD_FILE{$key}->{E6REGDATE}=$SMATCH{$full_key}->{Regdate};
	$LOAD_FILE{$key}->{E6SchDate}=$SMATCH{$full_key}->{Plandate};
	$LOAD_FILE{$key}->{E6EXDATE}=$SMATCH{$full_key}->{Examdate};
	$LOAD_FILE{$key}->{E6Score}=$SMATCH{$full_key}->{ExamScore};
	$LOAD_FILE{$key}->{E6PF}=$SMATCH{$full_key}->{ExamPF};
	$LOAD_FILE{$key}->{E6Cert}=$SMATCH{$full_key}->{Cert};
	$LOAD_FILE{$key}->{E6APPDATE}=$SMATCH{$full_key}->{Appdate};


}
sub Create_LOADFILE_E7{
	$LOAD_FILE{$key}->{CSID}=$key;
	$LOAD_FILE{$key}->{Email}=$SMATCH{$full_key}->{Email};
	$LOAD_FILE{$key}->{FName}=$SMATCH{$full_key}->{FName};
	$LOAD_FILE{$key}->{LName}=$SMATCH{$full_key}->{LName};
	$LOAD_FILE{$key}->{E7REGDATE}=$SMATCH{$full_key}->{Regdate};
	$LOAD_FILE{$key}->{E7SchDate}=$SMATCH{$full_key}->{Plandate};
	$LOAD_FILE{$key}->{E7EXDATE}=$SMATCH{$full_key}->{Examdate};
	$LOAD_FILE{$key}->{E7Score}=$SMATCH{$full_key}->{ExamScore};
	$LOAD_FILE{$key}->{E7PF}=$SMATCH{$full_key}->{ExamPF};
	$LOAD_FILE{$key}->{E7Cert}=$SMATCH{$full_key}->{Cert};
	$LOAD_FILE{$key}->{E7APPDATE}=$SMATCH{$full_key}->{Appdate};


}
sub Create_LOADFILE_E8{
	$LOAD_FILE{$key}->{CSID}=$key;
	$LOAD_FILE{$key}->{Email}=$SMATCH{$full_key}->{Email};
	$LOAD_FILE{$key}->{FName}=$SMATCH{$full_key}->{FName};
	$LOAD_FILE{$key}->{LName}=$SMATCH{$full_key}->{LName};
	$LOAD_FILE{$key}->{E8REGDATE}=$SMATCH{$full_key}->{Regdate};
	$LOAD_FILE{$key}->{E8SchDate}=$SMATCH{$full_key}->{Plandate};
	$LOAD_FILE{$key}->{E8EXDATE}=$SMATCH{$full_key}->{Examdate};
	$LOAD_FILE{$key}->{E8Score}=$SMATCH{$full_key}->{ExamScore};
	$LOAD_FILE{$key}->{E8PF}=$SMATCH{$full_key}->{ExamPF};
	$LOAD_FILE{$key}->{E8Cert}=$SMATCH{$full_key}->{Cert};
	$LOAD_FILE{$key}->{E8APPDATE}=$SMATCH{$full_key}->{Appdate};


}
sub Write_Load_File{
	foreach $key (sort keys %LOAD_FILE ){
		print OFILE "$LOAD_FILE{$key}->{CSID},$LOAD_FILE{$key}->{FName},$LOAD_FILE{$key}->{LName},$LOAD_FILE{$key}->{Email},";
		print OFILE "$LOAD_FILE{$key}->{E1Cert},$LOAD_FILE{$key}->{E1REGDATE},$LOAD_FILE{$key}->{E1APPDATE},$LOAD_FILE{$key}->{E1SchDate},$LOAD_FILE{$key}->{E1EXDATE},$LOAD_FILE{$key}->{E1Score},$LOAD_FILE{$key}->{E1PF},";
		print OFILE "$LOAD_FILE{$key}->{E2Cert},$LOAD_FILE{$key}->{E2REGDATE},$LOAD_FILE{$key}->{E2APPDATE},$LOAD_FILE{$key}->{E2SchDate},$LOAD_FILE{$key}->{E2EXDATE},$LOAD_FILE{$key}->{E2Score},$LOAD_FILE{$key}->{E2PF},";
		print OFILE "$LOAD_FILE{$key}->{E3Cert},$LOAD_FILE{$key}->{E3REGDATE},$LOAD_FILE{$key}->{E3APPDATE},$LOAD_FILE{$key}->{E3SchDate},$LOAD_FILE{$key}->{E3EXDATE},$LOAD_FILE{$key}->{E3Score},$LOAD_FILE{$key}->{E3PF},";
		print OFILE "$LOAD_FILE{$key}->{E4Cert},$LOAD_FILE{$key}->{E4REGDATE},$LOAD_FILE{$key}->{E4APPDATE},$LOAD_FILE{$key}->{E4SchDate},$LOAD_FILE{$key}->{E4EXDATE},$LOAD_FILE{$key}->{E4Score},$LOAD_FILE{$key}->{E4PF},";
		print OFILE "$LOAD_FILE{$key}->{E5Cert},$LOAD_FILE{$key}->{E5REGDATE},$LOAD_FILE{$key}->{E5APPDATE},$LOAD_FILE{$key}->{E5SchDate},$LOAD_FILE{$key}->{E5EXDATE},$LOAD_FILE{$key}->{E5Score},$LOAD_FILE{$key}->{E5PF},";
		print OFILE "$LOAD_FILE{$key}->{E6Cert},$LOAD_FILE{$key}->{E6REGDATE},$LOAD_FILE{$key}->{E6APPDATE},$LOAD_FILE{$key}->{E6SchDate},$LOAD_FILE{$key}->{E6EXDATE},$LOAD_FILE{$key}->{E6Score},$LOAD_FILE{$key}->{E6PF},";
		print OFILE "$LOAD_FILE{$key}->{E7Cert},$LOAD_FILE{$key}->{E7REGDATE},$LOAD_FILE{$key}->{E7APPDATE},$LOAD_FILE{$key}->{E7SchDate},$LOAD_FILE{$key}->{E7EXDATE},$LOAD_FILE{$key}->{E7Score},$LOAD_FILE{$key}->{E7PF},";
		print OFILE "$LOAD_FILE{$key}->{E8Cert},$LOAD_FILE{$key}->{E8REGDATE},$LOAD_FILE{$key}->{E8APPDATE},$LOAD_FILE{$key}->{E8SchDate},$LOAD_FILE{$key}->{E8EXDATE},$LOAD_FILE{$key}->{E8Score},$LOAD_FILE{$key}->{E8PF},EOL\n";

		print CFILE "INPUT,$LOAD_FILE{$key}->{CSID},$LOAD_FILE{$key}->{FName},$LOAD_FILE{$key}->{LName},$LOAD_FILE{$key}->{Email},";
		print CFILE "$LOAD_FILE{$key}->{E1Cert},$LOAD_FILE{$key}->{E1REGDATE},$LOAD_FILE{$key}->{E1APPDATE},$LOAD_FILE{$key}->{E1SchDate},$LOAD_FILE{$key}->{E1EXDATE},$LOAD_FILE{$key}->{E1Score},$LOAD_FILE{$key}->{E1PF},";
		print CFILE "$LOAD_FILE{$key}->{E2Cert},$LOAD_FILE{$key}->{E2REGDATE},$LOAD_FILE{$key}->{E2APPDATE},$LOAD_FILE{$key}->{E2SchDate},$LOAD_FILE{$key}->{E2EXDATE},$LOAD_FILE{$key}->{E2Score},$LOAD_FILE{$key}->{E2PF},";
		print CFILE "$LOAD_FILE{$key}->{E3Cert},$LOAD_FILE{$key}->{E3REGDATE},$LOAD_FILE{$key}->{E3APPDATE},$LOAD_FILE{$key}->{E3SchDate},$LOAD_FILE{$key}->{E3EXDATE},$LOAD_FILE{$key}->{E3Score},$LOAD_FILE{$key}->{E3PF},";
		print CFILE "$LOAD_FILE{$key}->{E4Cert},$LOAD_FILE{$key}->{E4REGDATE},$LOAD_FILE{$key}->{E4APPDATE},$LOAD_FILE{$key}->{E4SchDate},$LOAD_FILE{$key}->{E4EXDATE},$LOAD_FILE{$key}->{E4Score},$LOAD_FILE{$key}->{E4PF},";
		print CFILE "$LOAD_FILE{$key}->{E5Cert},$LOAD_FILE{$key}->{E5REGDATE},$LOAD_FILE{$key}->{E5APPDATE},$LOAD_FILE{$key}->{E5SchDate},$LOAD_FILE{$key}->{E5EXDATE},$LOAD_FILE{$key}->{E5Score},$LOAD_FILE{$key}->{E5PF},";
		print CFILE "$LOAD_FILE{$key}->{E6Cert},$LOAD_FILE{$key}->{E6REGDATE},$LOAD_FILE{$key}->{E6APPDATE},$LOAD_FILE{$key}->{E6SchDate},$LOAD_FILE{$key}->{E6EXDATE},$LOAD_FILE{$key}->{E6Score},$LOAD_FILE{$key}->{E6PF},";
		print CFILE "$LOAD_FILE{$key}->{E7Cert},$LOAD_FILE{$key}->{E7REGDATE},$LOAD_FILE{$key}->{E7APPDATE},$LOAD_FILE{$key}->{E7SchDate},$LOAD_FILE{$key}->{E7EXDATE},$LOAD_FILE{$key}->{E7Score},$LOAD_FILE{$key}->{E7PF},";
		print CFILE "$LOAD_FILE{$key}->{E8Cert},$LOAD_FILE{$key}->{E8REGDATE},$LOAD_FILE{$key}->{E8APPDATE},$LOAD_FILE{$key}->{E8SchDate},$LOAD_FILE{$key}->{E8EXDATE},$LOAD_FILE{$key}->{E8Score},$LOAD_FILE{$key}->{E8PF},EOL\n"
;
		print CFILE "SF,$SF_Data{$key}->{CSID},$SF_Data{$key}->{FName},$SF_Data{$key}->{LName},,";
		print CFILE "$SF_Data{$key}->{E1Cert},$SF_Data{$key}->{E1REGDATE},$SF_Data{$key}->{E1APPDATE},$SF_Data{$key}->{E1SchDate},$SF_Data{$key}->{E1EXDATE},$SF_Data{$key}->{E1Score},$SF_Data{$key}->{E1PF},";
		print CFILE "$SF_Data{$key}->{E2Cert},$SF_Data{$key}->{E2REGDATE},$SF_Data{$key}->{E2APPDATE},$SF_Data{$key}->{E2SchDate},$SF_Data{$key}->{E2EXDATE},$SF_Data{$key}->{E2Score},$SF_Data{$key}->{E2PF},";
		print CFILE "$SF_Data{$key}->{E3Cert},$SF_Data{$key}->{E3REGDATE},$SF_Data{$key}->{E3APPDATE},$SF_Data{$key}->{E3SchDate},$SF_Data{$key}->{E3EXDATE},$SF_Data{$key}->{E3Score},$SF_Data{$key}->{E3PF},";
		print CFILE "$SF_Data{$key}->{E4Cert},$SF_Data{$key}->{E4REGDATE},$SF_Data{$key}->{E4APPDATE},$SF_Data{$key}->{E4SchDate},$SF_Data{$key}->{E4EXDATE},$SF_Data{$key}->{E4Score},$SF_Data{$key}->{E4PF},";
		print CFILE "$SF_Data{$key}->{E5Cert},$SF_Data{$key}->{E5REGDATE},$SF_Data{$key}->{E5APPDATE},$SF_Data{$key}->{E5SchDate},$SF_Data{$key}->{E5EXDATE},$SF_Data{$key}->{E5Score},$SF_Data{$key}->{E5PF},";
		print CFILE "$SF_Data{$key}->{E6Cert},$SF_Data{$key}->{E6REGDATE},$SF_Data{$key}->{E6APPDATE},$SF_Data{$key}->{E6SchDate},$SF_Data{$key}->{E6EXDATE},$SF_Data{$key}->{E6Score},$SF_Data{$key}->{E6PF},";
		print CFILE "$SF_Data{$key}->{E7Cert},$SF_Data{$key}->{E7REGDATE},$SF_Data{$key}->{E7APPDATE},$SF_Data{$key}->{E7SchDate},$SF_Data{$key}->{E7EXDATE},$SF_Data{$key}->{E7Score},$SF_Data{$key}->{E7PF},";
		print CFILE "$SF_Data{$key}->{E8Cert},$SF_Data{$key}->{E8REGDATE},$SF_Data{$key}->{E8APPDATE},$SF_Data{$key}->{E8SchDate},$SF_Data{$key}->{E8EXDATE},$SF_Data{$key}->{E8Score},$SF_Data{$key}->{E8PF},EOL\n";
	}

}
sub Get_STUDENT_Safe_ID{
	foreach $Line (sort keys %Diff ){
		@Elements=split(/,/,$Line);
		
		$key=lc($Elements[2]);
		if (exists $CS_1stID{$key} || exists $CS_2ndID{$key} || exists $CS_3rdID{$key}){
			if ( exists $CS_1stID{$key} && !exists $CS_2ndID{$key} && !exists $CS_3rdID{$key}){
				Process_One_Contact();

			}else{ 
				Process_Multi_Contacts();
			}
		}else{ 
			if ( $EFILE_PRINT eq 0 ){
				EFILE_HEADERS();
			}

			print EFILE "$Line,EOL\n";
		}

	}
}

sub Process_One_Contact{
	print SFILE "$CS_1stID{$key}->{CS_ID},$Line,EOL\n";
	$DEntry{CS_ID}=$CS_1stID{$key}->{CS_ID};
	$DEntry{FName}=strip($Elements[0]);
	$DEntry{LName}=strip($Elements[1]);
	$DEntry{Email}=lc(strip($Elements[2]));
	if (strip($Elements[4]) eq "CPCT-A"){
		$DEntry{Cert}="CPCT";
	}else{
		$DEntry{Cert}=strip($Elements[4]);
	}

	$DEntry{Regdate}=strip($Elements[6]);
	$DEntry{OrderTaken}=strip($Elements[7]);
	$DEntry{Appdate}=strip($Elements[9]);
	$DEntry{Plandate}=strip($Elements[10]);
	$DEntry{Examdate}=strip($Elements[11]);
	$DEntry{ExamScore}=strip($Elements[12]);
	$DEntry{ExamPF}=strip($Elements[13]);

	$KEY=$DEntry{CS_ID} . "|" . $DEntry{Cert} ."|" . $DEntry{Regdate};
	if ( exists $SMATCH{$KEY} ){
		delete $SMATCH{$KEY} ;	
		if ( $SMERROR_PRINT eq 0 ){
			SMERROR_HEADERS();
		}

		print SMERROR "$KEY,$DEntry{FName},$DEntry{LName},$DEntry{Email}  There 2 entries in the diff for this KEY\n";
	}else{
		$SMATCH{$KEY}={%DEntry};
	}
	$SMATCH_LINE{$KEY}=$Line;
	
}
sub Process_Multi_Contacts{
#	print MFILE "$Line,EOL\n";
	$DEntry{CS_ID}=$CS_1stID{$key}->{CS_ID};
	$DEntry{FName}=strip($Elements[0]);
	$DEntry{LName}=strip($Elements[1]);
	$DEntry{Email}=lc(strip($Elements[2]));
	if (strip($Elements[4]) eq "CPCT-A"){
		$DEntry{Cert}="CPCT";
	}else{
		$DEntry{Cert}=strip($Elements[4]);
	}

	$DEntry{Regdate}=strip($Elements[6]);
	$DEntry{OrderTaken}=strip($Elements[7]);
	$DEntry{Appdate}=strip($Elements[9]);
	$DEntry{Plandate}=strip($Elements[10]);
	$DEntry{Examdate}=strip($Elements[11]);
	$DEntry{ExamScore}=strip($Elements[12]);
	$DEntry{ExamPF}=strip($Elements[13]);


	$KEY1=$CS_1stID{$key}->{PCODE} . "|" . $DEntry{Cert};
	$KEY2=$CS_2ndID{$key}->{PCODE} . "|" . $DEntry{Cert};
	$KEY3=$CS_3rdID{$key}->{PCODE} . "|" . $DEntry{Cert};
	$KEY_good="";
	$KEY_MULTI="";
	if (exists $COURSE_CERT{$KEY1}   ){
		$KEY_good="1";
	}
	 if (exists $COURSE_CERT{$KEY2}) {
		if ( $KEY_good eq "" ){
			$KEY_good="2";
		}else{
			$KEY_MULTI="Multi";
		}
	}
	 if (exists $COURSE_CERT{$KEY3} ){
					
		if ( $KEY_good eq "" ){
			$KEY_good="3";
		}else{
			if ( $MFILE_PRINT eq 0 ){
				MFILE_HEADERS();
			}


			$KEY_MULTI="Multi";
		}
	}

	if ( $KEY_good eq "Multi" ){
		print MFILE "$Line,EOL\n";
	}else{
		if ( $KEY_good eq "1" ){
			$DEntry{CS_ID}=$CS_1stID{$key}->{CS_ID};
			$KEY1=$DEntry{CS_ID} . "|" . $DEntry{Cert} ."|" . $DEntry{Regdate};
			$SMATCH{$KEY1}={%DEntry};
		}else{
			if ( $KEY_good eq "2" ){
				$DEntry{CS_ID}=$CS_2ndID{$key}->{CS_ID};
				$KEY2=$DEntry{CS_ID} . "|" . $DEntry{Cert} ."|" . $DEntry{Regdate};
				$SMATCH{$KEY2}={%DEntry};
			}else{
				if ( $KEY_good eq "3" ){
					$DEntry{CS_ID}=$CS_2ndID{$key}->{CS_ID};
					$KEY3=$DEntry{CS_ID} . "|" . $DEntry{Cert} ."|" . $DEntry{Regdate};
					$SMATCH{$KEY3}={%DEntry};
				}else{
				# no Cert Match
				if ( $NOCERTM_PRINT eq 0 ){
					NOCERTM_HEADERS();
				}

				print NOCERTM "$Line,EOL\n";
				}
			}
		}
	}
	

}



sub Difference{
	$Row=0;
	while ($Line = <IFILE>){
		chomp($Line);
		chomp($Line);
		$Row=$Row+1;

		if ( substr($Line,0,3) ne ",,,"){
			@Elements=split(/,/,$Line);
			$Line1=$Elements[0] . "," . $Elements[1] . "," . $Elements[2] . ","  .$Elements[3] . "," . $Elements[4] . "," . $Elements[5] . "," . $Elements[6] . "," . $Elements[7] . "," . $Elements[8] . "," . $Elements[9] . "," . $Elements[10] . "," . $Elements[11] . "," . $Elements[12] . "," . $Elements[13] . "," . $Elements[14] .",EOL";
			
	
			if ($Row eq 1  ){
				print DFILE "$Line1\n";
			}else{
				if ( exists $PFILE{$Line1} ){
					;
				}else{
					print DFILE "$Line1,EOL\n";

					$Diff{$Line1}={"X"};
					
					
				}	
			}	
		}
	}
}


##########################################################################################
#   subroutine to remove quotes         			                           #
############################################################################################
sub strip{
	my $out1=$_[0];
	$out1=~s/\"//g;
	return($out1);	
	}












sub Read_PFILE{

	while ($Line = <PFILE>){
		chomp($Line);
		chomp($Line);
		@Elements=split(/,/,$Line);
		$Line1=$Elements[0] . "," . $Elements[1] . "," . $Elements[2] . ","  .$Elements[3] . "," . $Elements[4] . "," . $Elements[5] . "," . $Elements[6] . "," . $Elements[7] . "," . $Elements[8] . "," . $Elements[9] . "," . $Elements[10] . "," . $Elements[11] . "," . $Elements[12] . "," . $Elements[13] . "," . $Elements[14] .",EOL";
		$PFILE{$Line1}='x';
	}


}

#########################
sub Get_Contact_info{
	print "\n\n";
	print " Input the CSV file to get the Case Safe ID from  ( no extension)\n";
	$tmpFile=<STDIN>;
	chomp($tmpFile);
	$ContactFILE=$tmpFile . ".csv";
	$ERRCFile="Error_" . $ContactFILE;
	
}

sub Read_Contact{

	while ($Line = <ConFILE>){
		chomp($Line);
		chomp($Line);
		@Elements=split(/,/,$Line);
		$Entry{ID}=strip($Elements[0]);
		$Entry{NHA_email}=strip($Elements[8]);
		$Entry{ALT_email}=strip($Elements[7]);
		$Entry{Fname}=strip($Elements[4]);
		$Entry{Lname}=strip($Elements[5]);
		$Entry{Email}=strip($Elements[6]);
		$Entry{CS_ID}=strip($Elements[2]);
		$Entry{PCODE}=strip($Elements[18]);
		@EMAILElements=split(/@/,$Entry{Email});
		@EMAILElements=split(/@/,$Entry{Email});
		#if ($EMAILElements[1] eq "medcertsemail.com" ){
		#	if ($Entry{ALT_email} ne ''){
		#		$Entry{Email}=$Entry{ALT_email};
		#		if ($Entry{NHA_email} ne ''){
		#			$Entry{ALT_email}=$Entry{NHA_email};
		#			$Entry{NHA_email}='';
		#		}else{
		#			$Entry{ALT_email}='';
		#		}
		#	}
		#}

		if ( $Entry{Email} eq $Entry{NHA_email} || $Entry{ALT_email} eq $Entry{NHA_email}){
			$Entry{NHA_email}='';
		}
		if ( $Entry{Email} eq $Entry{ALT_email}){
			$Entry{ALT_email}='';
		}
		@EMAILElements=split(/@/,$Entry{Email});
					
		if ( $Entry{Email} ne '' ){
			if ( !exists $CS_1stID{$Entry{Email}}){
				$CS_1stID{$Entry{Email}}={%Entry};
			}else{
				if ( !exists $CS_2ndID{$Entry{Email}}){
					$CS_2ndID{$Entry{Email}}={%Entry};
				}else{
					if ( !exists $CS_3rdID{$Entry{Email}}){
						$CS_3rdID{$Entry{Email}}={%Entry};
					}else{
						print	CERROR " $Line has issues with primary emails\n";
						
					}
				}
			}
		}
		@EMAILElements=split(/@/,$Entry{ALT_email});
		#if ( $Entry{ALT_email} ne '' && $EMAILElements[1] ne "medcertsemail.com" ){
		if ( $Entry{ALT_email} ne ''  ){
			if ( !exists $CS_1stID{$Entry{ALT_email}}){
				$CS_1stID{$Entry{ALT_email}}={%Entry};
			}else{
				if ( !exists $CS_2ndID{$Entry{ALT_email}}){
					$CS_2ndID{$Entry{ALT_email}}={%Entry};
				}else{
					if ( !exists $CS_3rdID{$Entry{ALT_email}}){
						$CS_3rdID{$Entry{ALT_email}}={%Entry};
					}else{
						print	CERROR " $Line has issues with Alt emails\n";
					}
				}
			}
		}
		@EMAILElements=split(/@/,$Entry{NHA_email});
		#if ( $Entry{NHA_email} ne '' && $EMAILElements[1] ne "medcertsemail.com" ){
		if ( $Entry{NHA_email} ne ''  ){
			if ( !exists $CS_1stID{$Entry{NHA_email}}){
				$CS_1stID{$Entry{NHA_email}}={%Entry};
			}else{
				if ( !exists $CS_2ndID{$Entry{NHA_email}}){
					$CS_2ndID{$Entry{NHA_email}}={%Entry};
				}else{
					if ( !exists $CS_3rdID{$Entry{NHA_email}}){
						$CS_3rdID{$Entry{NHA_email}}={%Entry};
					}else{
						print	CERROR " $Line has issues with NHA emails\n";
					}
				}
			}
		}


	}	


}

sub Read_SFFILE{

	while ($Line = <SFFILE>){
		chomp($Line);
		chomp($Line);
		$Row=$Row+1;
		@Elements=split(/,/,$Line);
		if ($Row gt 1 ){
			$DATE_COL=6;
			Format_Date();
			$DATE_COL=7;
			Format_Date();
			$DATE_COL=8;
			Format_Date();
			$DATE_COL=9;
			Format_Date();
			$DATE_COL=13;
			Format_Date();
			$DATE_COL=14;
			Format_Date();
			$DATE_COL=15;
			Format_Date();
			$DATE_COL=16;
			Format_Date();
			$DATE_COL=20;
			Format_Date();
			$DATE_COL=21;
			Format_Date();
			$DATE_COL=22;
			Format_Date();
			$DATE_COL=23;
			Format_Date();
			$DATE_COL=27;
			Format_Date();
			$DATE_COL=28;
			Format_Date();
			$DATE_COL=29;
			Format_Date();
			$DATE_COL=30;
			Format_Date();
			$DATE_COL=34;
			Format_Date();
			$DATE_COL=35;
			Format_Date();
			$DATE_COL=36;
			Format_Date();
			$DATE_COL=37;
			Format_Date();
			$DATE_COL=41;
			Format_Date();
			$DATE_COL=42;
			Format_Date();
			$DATE_COL=43;
			Format_Date();
			$DATE_COL=44;
			Format_Date();
			$DATE_COL=48;
			Format_Date();
			$DATE_COL=49;
			Format_Date();
			$DATE_COL=50;
			Format_Date();
			$DATE_COL=51;
			Format_Date();
			$DATE_COL=55;
			Format_Date();
			$DATE_COL=56;
			Format_Date();
			$DATE_COL=57;
			Format_Date();
			$DATE_COL=58;
			Format_Date();

			$Entry{CSID}=strip($Elements[0]);
			$key=strip($Elements[0]);
			$Entry{FName}=strip($Elements[2]);
			$Entry{LName}=strip($Elements[3]);
			$Entry{StudentID}=strip($Elements[1]);
			$SF_Data{$key}={%Entry};
			$SF_Data{$key}->{E1Cert}=strip($Elements[5]);
			$SF_Data{$key}->{E1REGDATE}=strip($Elements[6]);
			$SF_Data{$key}->{E1APPDATE}=strip($Elements[7]);
			$SF_Data{$key}->{E1SchDate}=strip($Elements[8]);
			$SF_Data{$key}->{E1EXDATE}=strip($Elements[9]);
			$SF_Data{$key}->{E1Score}=strip($Elements[10]);
			$SF_Data{$key}->{E1PF}=strip($Elements[11]);
			$SF_Data{$key}->{E2Cert}=strip($Elements[12]);
			$SF_Data{$key}->{E2REGDATE}=strip($Elements[13]);
			$SF_Data{$key}->{E2APPDATE}=strip($Elements[14]);
			$SF_Data{$key}->{E2SchDate}=strip($Elements[15]);
			$SF_Data{$key}->{E2EXDATE}=strip($Elements[16]);
			$SF_Data{$key}->{E2Score}=strip($Elements[17]);
			$SF_Data{$key}->{E2PF}=strip($Elements[18]);
			$SF_Data{$key}->{E3Cert}=strip($Elements[19]);
			$SF_Data{$key}->{E3REGDATE}=strip($Elements[20]);
			$SF_Data{$key}->{E3APPDATE}=strip($Elements[21]);
			$SF_Data{$key}->{E3SchDate}=strip($Elements[22]);
			$SF_Data{$key}->{E3EXDATE}=strip($Elements[23]);
			$SF_Data{$key}->{E3Score}=strip($Elements[24]);
			$SF_Data{$key}->{E3PF}=strip($Elements[25]);
			$SF_Data{$key}->{E4Cert}=strip($Elements[26]);
			$SF_Data{$key}->{E4REGDATE}=strip($Elements[27]);
			$SF_Data{$key}->{E4APPDATE}=strip($Elements[28]);
			$SF_Data{$key}->{E4SchDate}=strip($Elements[29]);
			$SF_Data{$key}->{E4EXDATE}=strip($Elements[30]);
			$SF_Data{$key}->{E4Score}=strip($Elements[31]);
			$SF_Data{$key}->{E4PF}=strip($Elements[32]);
			$SF_Data{$key}->{E5Cert}=strip($Elements[33]);
			$SF_Data{$key}->{E5REGDATE}=strip($Elements[34]);
			$SF_Data{$key}->{E5APPDATE}=strip($Elements[35]);
			$SF_Data{$key}->{E5SchDate}=strip($Elements[36]);
			$SF_Data{$key}->{E5EXDATE}=strip($Elements[37]);
			$SF_Data{$key}->{E5Score}=strip($Elements[38]);
			$SF_Data{$key}->{E5PF}=strip($Elements[39]);
			$SF_Data{$key}->{E6Cert}=strip($Elements[40]);
			$SF_Data{$key}->{E6REGDATE}=strip($Elements[41]);
			$SF_Data{$key}->{E6APPDATE}=strip($Elements[42]);
			$SF_Data{$key}->{E6SchDate}=strip($Elements[43]);
			$SF_Data{$key}->{E6EXDATE}=strip($Elements[44]);
			$SF_Data{$key}->{E6Score}=strip($Elements[45]);
			$SF_Data{$key}->{E6PF}=strip($Elements[46]);
			$SF_Data{$key}->{E7Cert}=strip($Elements[47]);
			$SF_Data{$key}->{E7REGDATE}=strip($Elements[48]);
			$SF_Data{$key}->{E7APPDATE}=strip($Elements[49]);
			$SF_Data{$key}->{E7SchDate}=strip($Elements[50]);
			$SF_Data{$key}->{E7EXDATE}=strip($Elements[51]);
			$SF_Data{$key}->{E7Score}=strip($Elements[52]);
			$SF_Data{$key}->{E7PF}=strip($Elements[53]);
			$SF_Data{$key}->{E8Cert}=strip($Elements[54]);
			$SF_Data{$key}->{E8REGDATE}=strip($Elements[55]);
			$SF_Data{$key}->{E8APPDATE}=strip($Elements[56]);
			$SF_Data{$key}->{E8SchDate}=strip($Elements[57]);
			$SF_Data{$key}->{E8EXDATE}=strip($Elements[58]);
			$SF_Data{$key}->{E8Score}=strip($Elements[59]);
			$SF_Data{$key}->{E8PF}=strip($Elements[60]);
			$SF_Data{$key}->{PCODE}=strip($Elements[62]);
			


			
		}
	}
				
}
sub Format_Date{
	$Date=strip($Elements[$DATE_COL]);
	if ( $Date != "" ){
				$Date=~s#-#/#g;
				@DElements=split(/\//,$Date);
				$MM=$DElements[0];
				if ( length($MM) == 1){
					$MM="0" . $MM;
				}
				$DD=$DElements[1];
				if ( length($DD) == 1){
					$DD="0" . $DD;
				}

				$YY=$DElements[2];
				if ( length($YY) == 2){
					$YY="20" . $YY;
				}
				$Elements[$DATE_COL]=$YY . "-" . $MM . "-" . $DD;
	}
}

sub HEADERS{
print OFILE "Case Safe ID,First Name,Last Name,Email,Certification 1 Registered Program,Certification 1 Registration Date,Certification 1 Approval Date,Certification 1 Scheduled Date,Certification 1 Testing Date 1,Certification 1 Exam Raw Score,Certification 1 Testing Result,Certification 2 Registered Program,Certification 2 Registration Date,Certification 2 Approval Date,Certification 2 Scheduled Date,Certification 2 Testing Date 1,Certification 2 Exam Raw Score,Certification 2 Testing Result 1,Certification 3 Registered Program,Certification 3 Registration Date,Certification 3 Approval Action Date,Certification 3 Scheduled Date,Certification 3 Testing Date,Certification 3 Exam Raw Score,Certification 3 Testing Result,Certification 4 Registered Program,Cert. Attempt 4 Registration Date,Certification 4 Approval Date,Certification 4 Scheduled Date,Certification 4 Testing Date,Certification 4 Exam Raw Score,Certification 4 Final Result,";
print OFILE "Cert. Attempt 5 Exam,Cert. Attempt 5 Registration Date,Cert. Attempt 5 Approval Date,Cert. Attempt 5 Scheduled Date,Cert. Attempt 5 Testing Date,Cert. Attempt 5 Exam Raw Score,Certification 5 Testing Result,Cert. Attempt 6 Exam,Cert. Attempt 6 Registration Date,Cert. Attempt 6 Approval Date,Cert. Attempt 6 Scheduled Date,Cert. Attempt 6 Testing Date,Cert. Attempt 6 Exam Raw Score,Certification 6 Testing Result,Cert. Attempt 7 Exam,Cert. Attempt 7 Registration Date,Cert. Attempt 7 Approval Date,Cert. Attempt 7 Scheduled Date,Cert. Attempt 7 Testing Date,Cert. Attempt 7 Exam Raw Score,Certification 7 Testing Result,Cert. Attempt 8 Exam,Cert. Attempt 8 Registration Date,Cert. Attempt 8 Approval Date,Cert. Attempt 8 Scheduled Date,Cert. Attempt 8 Testing Date,Cert. Attempt 8 Exam Raw Score,Certification 8 Testing Result,EOL\n";
print CFILE "Source,Case Safe ID,First Name,Last Name,Email,Certification 1 Registered Program,Certification 1 Registration Date,Certification 1 Approval Date,Certification 1 Scheduled Date,Certification 1 Testing Date 1,Certification 1 Exam Raw Score,Certification 1 Testing Result,Certification 2 Registered Program,Certification 2 Registration Date,Certification 2 Approval Date,Certification 2 Scheduled Date,Certification 2 Testing Date 1,Certification 2 Exam Raw Score,Certification 2 Testing Result 1,Certification 3 Registered Program,Certification 3 Registration Date,Certification 3 Approval Action Date,Certification 3 Scheduled Date,Certification 3 Testing Date,Certification 3 Exam Raw Score,Certification 3 Testing Result,Certification 4 Registered Program,Cert. Attempt 4 Registration Date,Certification 4 Approval Date,Certification 4 Scheduled Date,Certification 4 Testing Date,Certification 4 Exam Raw Score,Certification 4 Final Result,";
print CFILE "Cert. Attempt 5 Exam,Cert. Attempt 5 Registration Date,Cert. Attempt 5 Approval Date,Cert. Attempt 5 Scheduled Date,Cert. Attempt 5 Testing Date,Cert. Attempt 5 Exam Raw Score,Certification 5 Testing Result,Cert. Attempt 6 Exam,Cert. Attempt 6 Registration Date,Cert. Attempt 6 Approval Date,Cert. Attempt 6 Scheduled Date,Cert. Attempt 6 Testing Date,Cert. Attempt 6 Exam Raw Score,Certification 6 Testing Result,Cert. Attempt 7 Exam,Cert. Attempt 7 Registration Date,Cert. Attempt 7 Approval Date,Cert. Attempt 7 Scheduled Date,Cert. Attempt 7 Testing Date,Cert. Attempt 7 Exam Raw Score,Certification 7 Testing Result,Cert. Attempt 8 Exam,Cert. Attempt 8 Registration Date,Cert. Attempt 8 Approval Date,Cert. Attempt 8 Scheduled Date,Cert. Attempt 8 Testing Date,Cert. Attempt 8 Exam Raw Score,Certification 8 Testing Result,EOL\n";
print AFILE "First Name,Last Name,Student Email,Institution,Cert/Product,ProductNumber,NHA Exam Registration Date,Exam Order Taken,Mode of Testing,Approval Date,Planned Exam Date,Actual Exam Date,Score,Exam Result,Certification # Awarded,Data Current As Of,EOL\n";
print SFILE "Case Safe ID,First Name,Last Name,Student Email,Institution,Cert/Product,ProductNumber,NHA Exam Registration Date,Exam Order Taken,Mode of Testing,Approval Date,Planned Exam Date,Actual Exam Date,Score,Exam Result,Certification # Awarded,Data Current As Of,EOL\n";

}
sub NOSPOT_HEADERS{
	open(NOSPOT,">$NO_SPOT_FOUND") or die "File $$NO_SPOT_FOUND can not be open";
	print NOSPOT "First Name,Last Name,Student Email,Institution,Cert/Product,ProductNumber,NHA Exam Registration Date,Exam Order Taken,Mode of Testing,Approval Date,Planned Exam Date,Actual Exam Date,Score,Exam Result,Certification # Awarded,Data Current As Of,EOL\n";
	$NOSPOT_PRINT=1;
}
sub NOCERTM_HEADERS{
	open(NOCERTM,">$NO_CERT_MATCH") or die "File $NO_CERT_MATCH can not be open";
	print NOCERTM  "First Name,Last Name,Student Email,Institution,Cert/Product,ProductNumber,NHA Exam Registration Date,Exam Order Taken,Mode of Testing,Approval Date,Planned Exam Date,Actual Exam Date,Score,Exam Result,Certification # Awarded,Data Current As Of,EOL\n";
	$NOCERTM_PRINT=1;
}
sub SMERROR_HEADERS{
	open(SMERROR,">$SMERROR_FILE") or die "File $SMERROR_FILE can not be open";
	print SMERROR "First Name,Last Name,Student Email,Institution,Cert/Product,ProductNumber,NHA Exam Registration Date,Exam Order Taken,Mode of Testing,Approval Date,Planned Exam Date,Actual Exam Date,Score,Exam Result,Certification # Awarded,Data Current As Of,EOL\n";
	$SMERROR_PRINT=1;

}

sub EFILE_HEADERS{
	open(EFILE,">$ERRORFILE") or die "File $ERRORFILE can not be open";
	print EFILE "First Name,Last Name,Student Email,Institution,Cert/Product,ProductNumber,NHA Exam Registration Date,Exam Order Taken,Mode of Testing,Approval Date,Planned Exam Date,Actual Exam Date,Score,Exam Result,Certification # Awarded,Data Current As Of,EOL\n";
	$EFILE_PRINT=1;

}
sub MFILE_HEADERS{
	open(MFILE,">$Multi_match") or die "File $Multi_match can not be open";
	print MFILE "First Name,Last Name,Student Email,Institution,Cert/Product,ProductNumber,NHA Exam Registration Date,Exam Order Taken,Mode of Testing,Approval Date,Planned Exam Date,Actual Exam Date,Score,Exam Result,Certification # Awarded,Data Current As Of,EOL\n";
	$MFILE_PRINT=1;

}


