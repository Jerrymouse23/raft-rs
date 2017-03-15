source ./var.sh

normal=$'\e[0m'  
green=$(tput setaf 2) 
red="$bold$(tput setaf 1)" 

curl --fail --verbose -X POST -c session_cookie -H "Content-Type: application/json" -H "Cache-Control: no-cache" -d '{
        "username":"'$username'",
	"password":"'$plain_password'"
	}' "$url/auth/login" && echo "$green login successful" || echo "$red failed login" 

echo "$normal"
sleep 1

doc_id=$(curl --fail --verbose -b session_cookie -X POST -H "Content-Type: application/json" -H "Cache-Control: no-cache" -d '{
        "payload":"dGVzVA==",
	 "version":1,
	"id":"wuarscht wird ersetzt"
}' "$url/document/$lid" ) && echo "$green document post successful" || echo "$red failed to  post data"

echo "$normal"
sleep 2 

curl --fail --verbose -b session_cookie -X GET "$url/document/$lid/$doc_id" && echo "$green document get successful" || echo "$red failed to get document"

echo "$normal"
sleep 1

curl --fail --verbose -b session_cookie -X PUT -H "Content-Type: application/json" -H "Cache-Control: no-cache" -d '{
	"payload":"dXBkYXRlZA=="
}' "$url/document/$lid/document/$doc_id" && echo "$green updating document successful" || echo "$red updating document failed"

echo "$normal"
sleep 1

curl --fail --verbose -b session_cookie -X DELETE "$url/document/$lid/$doc_id" && echo "$green deleting document successful" || echo "$red deleting document failed"

echo "$normal"
sleep 1

curl --fail --verbose -b session_cookie -X GET "$url/meta/log/$lid/documents" && echo "$green fetching all documents was successful" || echo "$red failed to fetch all documents"



