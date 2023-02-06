package main

import (
	"bufio"
	"fmt"
	"net/http"
	"os"
	"path/filepath"

	"github.com/joho/godotenv"
)

var load_env = godotenv.Load(filepath.Join("/home/rafzul/projects/entsoe-pipelines", ".env"))

func main() {
	key := os.Getenv("OPENWEATHER_APIKEY")
	// fmt.Println(key)
	url := fmt.Sprintf("https://api.openweathermap.org/data/2.5/weather?lat=-44.34&lon=10.99&appid=%s", key)
	resp, err := http.Get(url)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	fmt.Println("Response status:", resp.Status)

	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		response_text := scanner.Text()
		fmt.Println(response_text)
	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}

}
