package main

import (
	"fmt"
	"os"

	"github.com/joho/godotenv"
)

func main() {
	godotenv.Load(".env")
	var key string = os.Getenv("OPENWEATHER_APIKEY")
	fmt.Println(key)
	url := "https://api.openweathermap.org/data/2.5/weather?lat=-7.716843271640896&lon=109.00909741089343&appid=" + key
	fmt.Println(url)
	// resp, err := http.Get(url)
	// if err != nil {
	// 	panic(err)
	// }
	// defer resp.Body.Close()
	// fmt.Println("Response status:", resp.Status)
}
