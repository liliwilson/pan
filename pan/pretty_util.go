package pan

import (
	"fmt"
	"log"
	"os"
	"pan/panapi/rpc"
	"strconv"
	"strings"
	"time"
)

type logTopic string

const (
	dCreate      logTopic = "CRTE" // dClient  logTopic = "CLNT"
	dDelete      logTopic = "DLTE" // dCommit  logTopic = "CMIT"
	dExists      logTopic = "EXST" // dDrop    logTopic = "DROP"
	dGetData     logTopic = "GETD" // dLeader  logTopic = "LEAD"
	dError       logTopic = "ERRO"
	dSetData     logTopic = "SETD" // dInfo    logTopic = "INFO"
	dGetChildren logTopic = "GETC" // dLog     logTopic = "LOG1"
	dWatch       logTopic = "WATC" // dLog2        logTopic = "LOG2"
	dEndSession  logTopic = "ENDS" // dPersist     logTopic = "PERS"
	dTimeout     logTopic = "TIME" // dSnap        logTopic = "SNAP"
	dEphemeral   logTopic = "DEPH" // dTerm        logTopic = "TERM"
	dTest        logTopic = "TEST"
	dTimer       logTopic = "TIMR"
	dTrace       logTopic = "TRCE"
	dWarn        logTopic = "WARN"
)

// Retrieve the verbosity level from an environment variable
func getVerbosity() int {
	v := os.Getenv("VERB")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

var debugStart time.Time
var debugVerbosity int

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func DebugPrint(topic logTopic, format string, a ...interface{}) {
	if debugVerbosity >= 1 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}

func PrintFlags(flags rpc.Flag) string {
	output := []string{}
	if flags.Ephemeral {
		output = append(output, "EPH")
	}
	if flags.Sequential {
		output = append(output, "SEQ")
	}
	outStr := strings.Join(output, ", ")
	if outStr == "" {
		return outStr
	}
	return "(" + outStr + ")"

}
