package main

import (
  "log"
  "math"
  "math/rand"
  "sync"
  "time"
)

type server struct {
  capacity         int
  requestCh        chan requestMsg
  quitCh           chan quitMsg
  requestHandlerCh chan request
  requests         []request
  vmR              map[int]float64
  vmL              map[int]float64
  vmP              map[int]float64
  activeIoNum      int
  maxQueueDepth    int
  lock             sync.Mutex
}

type requestMsg struct {
  vm int
  r  int
  l  int
  w  int
}

type request struct {
  vm int

  // Reservation tag
  R float64

  // Limit tag
  L float64

  // Share based tag
  P float64
}

type okMsg struct{}

type quitMsg struct {
  ok chan okMsg
}

func (s *server) run() {
  requestHandlerQuitCh := make(chan quitMsg)
  go s.handleRequest(s.capacity, requestHandlerQuitCh)
  var q quitMsg
L:
  for {
    select {
    case q = <-s.quitCh:
      break L
    case requestMsg := <-s.requestCh:
      s.requestArrival(requestMsg)
    }
  }
  requestHandlerQuitMsg := quitMsg{make(chan okMsg)}
  requestHandlerQuitCh <- requestHandlerQuitMsg
  <-requestHandlerQuitMsg.ok
  <-q.ok
  return
}

func (s *server) handleRequest(capacity int, quitCh chan quitMsg) {
  var q quitMsg
  sleepInterval := int(1000000.0 / float64(capacity))
L:
  for {
    select {
    case r := <-s.requestHandlerCh:
      t1 := time.Now().UnixMicro()
      go s.requestCompletion(r)
      interval := int(time.Now().UnixMicro() - t1)
      if interval < sleepInterval {
        time.Sleep(time.Duration(sleepInterval-interval) * time.Microsecond)
      }
    case q = <-quitCh:
      break L
    default:
      time.Sleep(time.Duration(sleepInterval) * time.Microsecond)
    }
  }
  q.ok <- okMsg{}
}

func (s *server) requestArrival(msg requestMsg) {
  s.lock.Lock()
  log.Printf("Request arrival %v\n", msg)
  vm := msg.vm
  _, exist := s.vmR[vm]
  t := float64(time.Now().UnixMicro()) / 1000.0
  var R float64
  var L float64
  var P float64
  if !exist {
    if len(s.requests) != 0 {
      minPtag := math.Inf(1)
      for _, r := range s.requests {
        if minPtag > r.P {
          minPtag = r.P
        }
      }
      for _, r := range s.requests {
        r.P -= minPtag - t
      }
    }
    R = t
    L = t
    P = t
  } else {
    R = math.Max(s.vmR[vm]+1.0/float64(msg.r), t)
    L = math.Max(s.vmL[vm]+1.0/float64(msg.l), t)
    P = math.Max(s.vmP[vm]+1.0/float64(msg.w), t)
  }
  s.vmR[vm] = R
  s.vmL[vm] = L
  s.vmP[vm] = P
  r := request{vm, R, L, P}
  s.requests = append(s.requests, r)
  s.lock.Unlock()
  s.scheduleRequest()
}

func (s *server) requestCompletion(r request) {
  s.lock.Lock()
  log.Printf("Request completion %v\n", r)
  s.activeIoNum -= 1
  s.lock.Unlock()
  s.scheduleRequest()
}

func (s *server) scheduleRequest() {
  s.lock.Lock()
  if s.activeIoNum >= s.maxQueueDepth {
    s.lock.Unlock()
    return
  }
  if len(s.requests) == 0 {
    s.lock.Unlock()
    return
  }
  t := float64(time.Now().UnixMicro()) / 1000.0
  minRtag := math.Inf(1)
  index := -1
  var selectedRequest request
  for i, r := range s.requests {
    if r.R <= t && r.R < minRtag {
      minRtag = t
      index = i
    }
  }
  if index != -1 {
    selectedRequest = s.requests[index]
    s.requests = append(s.requests[:index], s.requests[index+1:]...)
  } else {
    Ep := make([]int, 0)
    for i, r := range s.requests {
      if r.L <= t {
        Ep = append(Ep, i)
      }
    }
    if len(Ep) > 0 || s.activeIoNum == 0 {
      minPtag := math.Inf(1)
      for i := range Ep {
        ptag := s.requests[Ep[i]].P
        if ptag < minPtag {
          minPtag = ptag
          index = Ep[i]
        }
      }
    }
    if index != -1 {
      selectedRequest = s.requests[index]
      s.requests = append(s.requests[:index], s.requests[index+1:]...)
      vm := selectedRequest.vm
      for _, r := range s.requests {
        if r.vm == vm {
          r.R -= 1000.0 / s.vmR[vm]
        }
      }
    }
  }

  if index != -1 {
    s.activeIoNum += 1
  }
  s.lock.Unlock()
  s.requestHandlerCh <- selectedRequest
}

func clientType1(requestCh chan requestMsg, vm int, r int, l int, w int, iops int) chan okMsg {
  start := time.Now().UnixMilli()
  interval := 1000000 / iops
  okCh := make(chan okMsg)
  go func() {
    for {
      t := time.Now().UnixMilli()
      if t-start > 60*1000 {
        break
      }
      msg := requestMsg{vm, r, l, w}
      requestCh <- msg
      time.Sleep(time.Duration(interval) * time.Microsecond)
    }
    log.Printf("Client %v end\n", vm)
    okCh <- okMsg{}
  }()
  return okCh
}

func clientType2(requestCh chan requestMsg, vm int, r int, l int, w int, iopsMean int, iopsDev int) chan okMsg {
  start := time.Now().UnixMilli()
  sec := int64(0)
  iops := iopsMean
  interval := 1000000 / iops
  okCh := make(chan okMsg)
  go func() {
    for {
      t := time.Now().UnixMilli()
      if t-start > 60*1000 {
        break
      }
      if t-start > sec*1000 {
        sec += 1
        iops = int(rand.NormFloat64()*float64(iopsDev)) + iopsMean
        if iops < 0 {
          iops = 1
        }
        interval = 1000000 / iops
      }
      msg := requestMsg{vm, r, l, w}
      requestCh <- msg
      time.Sleep(time.Duration(interval) * time.Microsecond)
    }
    okCh <- okMsg{}
  }()
  return okCh
}

func main() {
  log.SetFlags(log.LstdFlags | log.Lshortfile)
  s := server{1000,
    make(chan requestMsg),
    make(chan quitMsg),
    make(chan request),
    make([]request, 0),
    make(map[int]float64),
    make(map[int]float64),
    make(map[int]float64),
    0,
    128,
    sync.Mutex{}}
  serverRequestCh := s.requestCh

  go s.run()
  client1Ch := clientType1(serverRequestCh, 1, 200, 400, 1, 150)
  client2Ch := clientType1(serverRequestCh, 2, 150, 400, 2, 500)
  client3Ch := clientType2(serverRequestCh, 3, 300, 600, 1, 500, 200)
  <-client1Ch
  <-client2Ch
  <-client3Ch
}
