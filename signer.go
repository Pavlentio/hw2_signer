package main

import (
    "fmt"
    "sort"
    "strconv"
    "strings"
    "sync"
)

const TH = 6

func ExecutePipeline(jobs ...job) {
    wg := &sync.WaitGroup{}
    in := make(chan interface{})
    for _, job := range jobs {
        wg.Add(1)
        out := make(chan interface{})
        go worker(job, in, out, wg)
        in = out
    }
    wg.Wait()
}

func worker(job job, in, out chan interface{}, wg *sync.WaitGroup) {
    defer wg.Done()
    defer close(out)
    job(in, out)
}

var SingleHash = func(in, out chan interface{}) {
    mu := &sync.Mutex{}
    wg := &sync.WaitGroup{}

    for shjob := range in {
        wg.Add(1)
        go SingleHashWorker(shjob, out, mu, wg)
    }

    wg.Wait()

}

func SingleHashWorker(in interface{}, out chan interface{}, mu *sync.Mutex, wg *sync.WaitGroup) {
    defer wg.Done()

    data := strconv.Itoa(in.(int))

    mu.Lock()
    md5 := DataSignerMd5(data)
    mu.Unlock()

    crc32chan := make(chan string)
    crc32md5chan := make(chan string)
    go crc32worker(data, crc32chan)
    go crc32worker(md5, crc32md5chan)
    crc32 := <-crc32chan
    crc32md5 := <-crc32md5chan

    result := crc32 + "~" + crc32md5

    fmt.Printf("%s SingleHash data %s\n", data, data)
    fmt.Printf("%s SingleHash md5(data) %s\n", data, md5)
    fmt.Printf("%s SingleHash crc32(md5(data)) %s\n", data, crc32md5)
    fmt.Printf("%s SingleHash crc32(data) %s\n", data, crc32)
    fmt.Printf("%s SingleHash result %s\n", data, result)

    out <- result
}

func crc32worker(data string, out chan string) {
    out <- DataSignerCrc32(data)
}

var MultiHash = func(in, out chan interface{}) {
    wg := &sync.WaitGroup{}

    for mhjob := range in {
        wg.Add(1)
        go MultiHashWorker(mhjob, out, wg)
    }

    wg.Wait()
}

func MultiHashWorker(in interface{}, out chan interface{}, wg *sync.WaitGroup) {
    defer wg.Done()

    mu := &sync.Mutex{}
    wgcrc32 := &sync.WaitGroup{}

    arr := make([]string, TH)

    for i := 0; i < TH; i++ {
        wgcrc32.Add(1)
        data := strconv.Itoa(i) + in.(string)
        go func(arr []string, data string, i int, mu *sync.Mutex, wg *sync.WaitGroup) {
            defer wg.Done()

            data = DataSignerCrc32(data)
            mu.Lock()
            arr[i] = data
            fmt.Printf("%s MultiHash: crc32(th+step1)) %d %s\n", in, i, data)
            mu.Unlock()
        }(arr, data, i, mu, wgcrc32)
    }

    wgcrc32.Wait()

    result := strings.Join(arr, "")
    fmt.Printf("%s MultiHash result: %s\n", in, result)
    out <- result
}

var CombineResults = func(in, out chan interface{}) {
    var arr []string

    for i := range in {

        arr = append(arr, i.(string))
    }

    sort.Strings(arr)
    result := strings.Join(arr, "_")

    fmt.Printf("CombineResults \n%s\n", result)

    out <- result
}
