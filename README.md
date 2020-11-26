# BioThings SDK Plugin for PubChem 3D Conformers 

Workflow in parsing file, after all files downloaded, is:  

1). Get all downloaded CSV CID pair files 

2). Make CSV files for filtered pairs - CID1 and CID2

3). Filter and add original CID1/CID2 pairs into created CSV files

			- Filter CID pairs such that the ST or CT values are above 96 (high similarity) 
		and below 101 (same compound)
    
4). Sort CSV files by CID1

5). Loop through all new CSV pair files and yield objects as: 

annotation = {
	  "_id" : CID1,
		"similar_conformers" : [CID2, CID2, CID2,...]
}

