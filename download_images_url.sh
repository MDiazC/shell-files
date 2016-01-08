path_to_data="$1"
if [ -z "$path_to_data" ]; then
	path_to_data="$(pwd)"
fi

function parse() {
	awk -v error_file=$3 -v output_file=$2 -v hotel_id=$4 -v url=$5 -v guid=$6 '
	BEGIN {Open=0;HasPhoto=0;}

	/];/ {if(Open == 1){ Open=0; } }
	/.*/ {if(Open == 1){
			HasPhoto=1;
			#extract the url of the image
			gsub(/^, /,"");
			#extract the id of the image
			n=split($0,a,"/");
			split(a[n],b,".");
			id=b[1];
			printf("INSERT IGNORE INTO booking_hotels_url_images (guid, image_id, hotel_id, image_url, refer_url) VALUES (%d, %d, %d, %s, \"%s\"); \n",guid,id,hotel_id,$0,url) > output_file ;
		}
	}
	/^var slideshow_photos/ { Open=1; }

	END {
		if(HasPhoto==0){
			printf("No se encontraron fotos en el archivo con url %s \n",url) > error_file;
		}
	}
' $1;
}

function insert_database(){

	echo "Saving the content of file $output_download_sql in database troovel_replica";
	/usr/local/mysql/bin/mysql -u"XXXX" -p"XXXX" --database troovel_replica < $1

	echo "Saving the content of $output_parse_sql in database troovel replica";
	/usr/local/mysql/bin/mysql -u"XXXX" -p"XXXX" --database troovel_social < $2

}

function parse_main() {

	if [ -d $2/downloaded_files ]; then
            rm -r $2/downloaded_files
        fi
	mkdir $2/downloaded_files

	output_folder="$2/downloaded_files"
	error_file="$2/error.log"
	output_download_sql="$2/booking_downloaded_files.sql"
	output_parse_sql="$2/booking_url_images.sql"
	output_parse_tmp="$2/temp_output_files"
	error_parse_tmp="$2/temp_error"
	REGEX="^http"
	i=1;

	#empty the output files
	cat /dev/null > $output_download_sql
	cat /dev/null > $output_parse_sql
	cat /dev/null > $error_file

	#read line by line the file pass as the first parameter
	while read -r line
	do
		#if the line starts with "http"... enter
		if [[ $line =~ $REGEX ]]
		then
			#split the line between the url and the hotel_id
			array=($line)
			url=${array[0]}
			hotel_id=${array[1]}
			guid=${array[2]}

			echo "Output will be generated at  $output_folder/web_booking_hotel_id_$hotel_id"
			echo "process website: $url"
			echo "INSERT IGNORE INTO booking_hotels_downloaded_file (hotel_id, file_path) VALUES ($hotel_id, \"$output_folder/web_booking_hotel_id_$hotel_id\");" >> $output_download_sql
			sleep $(($RANDOM%3))
			#donwload the web and save it in the folder "$output_folder"
			wget -erobots=off -O $output_folder/web_booking_hotel_id_$hotel_id -U Mozilla $url --random-wait -t 3
			#after the download we check if the file exists
			if [ -f $output_folder/web_booking_hotel_id_$hotel_id ] 
			then
				echo "Parsing file $output_folder/web_booking_hotel_id_$hotel_id"
				#if file exists parse the file, if the parse is correct we save the INSERT sql in "output_parse_tmp_file" if not we save the error in "error_parse_tmp_file"
				parse $output_folder/web_booking_hotel_id_$hotel_id $output_parse_tmp $error_parse_tmp $hotel_id $url"?aid=398495" $guid
				#save de result of the parse mixing the result of the parse with the previous parses
				if [ -f $output_parse_tmp ] 
				then
					cat $output_parse_tmp >> $output_parse_sql
				fi
				#save the errors of the parse mixing the result of the error with the previous error
				if [ -f $error_parse_tmp ] 
				then
					cat $error_parse_tmp >> $error_file
				fi

			else
				echo "Cannot open file $output_folder/web_booking_hotel_id_$hotel_id" >> $error_file
				continue;
			fi
		else
			echo "Cannot process the line '$line' don't seems a url valid" >> $error_file
			continue;
		fi
		echo -e "\n"
		cat /dev/null > $output_parse_tmp
		cat /dev/null > $error_parse_tmp

		if [ `expr $i % 1000` = 0 ];	then echo "------------> "$i" entities"; sleep 1m; fi
		i=`expr $i + 1`;

	done < "$2/$1"

	#delete temporal file for parse
	if [ -f  $output_parse_tmp ]
	then
		rm  $output_parse_tmp;
	fi
	#delete temporal file for error
	if [ -f $error_parse_tmp ]
	then
		rm $error_parse_tmp;
	fi

	insert_database $output_download_sql $output_parse_sql

	rm list_booking_url
}

echo "Iniciando script... "

#Create a file with the url of the hotel and the hotel id through the next sql, we save the results in the file "list_booking_url" and call the main function
#/usr/local/mysql/bin/mysql -u"tsocial" -p"KSaA8shHj2" --database troovel_replica -e "SELECT h.url, h.hotel_id, m.guid FROM troovel_replica.booking_hotels AS h INNER JOIN troovel_social.mapping AS m ON m.external_id = h.hotel_id WHERE h.hotel_id NOT IN(SELECT hotel_id FROM troovel_replica.booking_hotels_downloaded_file) AND m.source='BOOKING'" > list_booking_url

echo "URL's extraidas de la base de datos"

parse_main list_booking_url $path_to_data

