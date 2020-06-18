## Zillow_Quandl
grep 'https://www.quandl.com/api/v3/datasets/ZILLOW/Z.*_MSP.*' zillowheaders.txt | tail -r >> zillowMSP.txt
## in the folder
wget -nv -w 0.2 -i ../zillowMSP.txt
autoload zmv
zmv '(*)?api_key=8xeaBs12HrsxzJjuqszW' '${1}.json'
aws s3 sync . s3://disaster.opportunity/Zill_Quandl_Data/MSP_Data/
