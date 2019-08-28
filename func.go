package main

import (
	"strings"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/oracle/oci-go-sdk/common"
	"github.com/oracle/oci-go-sdk/common/auth"
	"github.com/oracle/oci-go-sdk/objectstorage"
	"github.com/Sugi275/oci-objectstorage-multiregioner/loglib"
	fdk "github.com/fnproject/fdk-go"
)

const (
	envBucketName = "OCI_BUCKETNAME"
	envSourceRegion = "OCI_SOURCE_REGION"
	envDestinationRegions = "OCI_DESTINATION_REGIONS"
	actionTypeCreate = "com.oraclecloud.objectstorage.createobject"
	actionTypeUpdate = "com.oraclecloud.objectstorage.updateobject"
	actionTypeDelete = "com.oraclecloud.objectstorage.deleteobject"
)

// EventsInput EventsInput
type EventsInput struct {
	CloudEventsVersion string      `json:"cloudEventsVersion"`
	EventID            string      `json:"eventID"`
	EventType          string      `json:"eventType"`
	Source             string      `json:"source"`
	EventTypeVersion   string      `json:"eventTypeVersion"`
	EventTime          time.Time   `json:"eventTime"`
	SchemaURL          interface{} `json:"schemaURL"`
	ContentType        string      `json:"contentType"`
	Extensions         struct {
		CompartmentID string `json:"compartmentId"`
	} `json:"extensions"`
	Data struct {
		CompartmentID      string `json:"compartmentId"`
		CompartmentName    string `json:"compartmentName"`
		ResourceName       string `json:"resourceName"`
		ResourceID         string `json:"resourceId"`
		AvailabilityDomain string `json:"availabilityDomain"`
		FreeFormTags       struct {
			Department string `json:"Department"`
		} `json:"freeFormTags"`
		DefinedTags struct {
			Operations struct {
				CostCenter string `json:"CostCenter"`
			} `json:"Operations"`
		} `json:"definedTags"`
		AdditionalDetails struct {
			Namespace        string `json:"namespace"`
			PublicAccessType string `json:"publicAccessType"`
			ETag             string `json:"eTag"`
		} `json:"additionalDetails"`
	} `json:"data"`
}

// Action Action
type Action struct {
	Namespace  string
	BucketName string
	ObjectName string
	ActionType string
	SourceRegion string
	DestinationRegions []string
	ctx context.Context
}

func main() {
	fdk.Handle(fdk.HandlerFunc(fnMain))

	// ------- local development ---------
	// reader := os.Stdin
	// writer := os.Stdout
	// fnMain(context.TODO(), reader, writer)
}

func fnMain(ctx context.Context, in io.Reader, out io.Writer) {
	loglib.InitSugar()
	defer loglib.Sugar.Sync()

	// Events から受け取るパラメータ
	input := &EventsInput{}
	json.NewDecoder(in).Decode(input)

	action, err := generateAction(ctx, *input)

	if err != nil {
		loglib.Sugar.Error(err)
		return
	}

	err = runAction(action)

	if err != nil {
		loglib.Sugar.Error(err)
		return
	}

	out.Write([]byte("Done!"))
}

func generateAction(ctx context.Context, input EventsInput) (Action, error) {
	action := Action{}

	var ok bool

	// Namespace
	action.Namespace = input.Data.AdditionalDetails.Namespace

	// BucketName
	var bucketName string
	if bucketName, ok = os.LookupEnv(envBucketName); !ok {
		err := fmt.Errorf("can not read envBucketName from environment variable %s", envBucketName)
		return action, err
	}
	action.BucketName = bucketName

	// ObjectName
	action.ObjectName = input.Data.ResourceName

	// ActionType
	action.ActionType = input.EventType

	// SourceRegion
	var sourceRegion string
	if sourceRegion, ok = os.LookupEnv(envSourceRegion); !ok {
		err := fmt.Errorf("can not read envSourceRegion from environment variable %s", envSourceRegion)
		return action, err
	}
	action.SourceRegion = sourceRegion

	// DestinationRegions
	var destinationRegionString string
	if destinationRegionString, ok = os.LookupEnv(envDestinationRegions); !ok {
		err := fmt.Errorf("can not read envDestinationRegions from environment variable %s", envDestinationRegions)
		return action, err
	}
	destinationRegions := strings.Split(destinationRegionString, ",")
	action.DestinationRegions = destinationRegions

	// Context
	action.ctx = ctx

	return action, nil
}

func runAction(action Action) error {
	var err error

	fmt.Println(action)

	switch action.ActionType {
	case actionTypeCreate, actionTypeUpdate:
		err = runCreate(action)
	case actionTypeDelete:
		err = runDelete(action)
	default:
		err = fmt.Errorf("do nothing. ActionType : %s", action.ActionType)
	}

	if err != nil {
		return err
	}

	return nil
}

func runCreate(action Action) error {
	provider, err := auth.ResourcePrincipalConfigurationProvider()

	if err != nil {
		loglib.Sugar.Error(err)
		return err
	}

	// provider := common.DefaultConfigProvider()
	client, err := objectstorage.NewObjectStorageClientWithConfigurationProvider(provider)
	client.SetRegion(string(action.SourceRegion)) 

	if err != nil {
		loglib.Sugar.Error(err)
		return err
	}

	for _, dest := range action.DestinationRegions {
		copyObjectDetail := objectstorage.CopyObjectDetails{
			SourceObjectName: common.String(action.ObjectName),
			DestinationRegion: common.String(dest),
			DestinationNamespace: common.String(action.Namespace),
			DestinationBucket: common.String(action.BucketName),
			DestinationObjectName: common.String(action.ObjectName),
		}
	
		request := objectstorage.CopyObjectRequest{
			NamespaceName: common.String(action.Namespace),
			BucketName: common.String(action.BucketName),
			CopyObjectDetails: copyObjectDetail,
		}
	
		fmt.Println(client)
		fmt.Println(request)

		_, err = client.CopyObject(action.ctx, request)
	
		if err != nil {
			loglib.Sugar.Error(err)
			return err
		}	
	}

	return nil
}

func runDelete(action Action) error {
	provider, err := auth.ResourcePrincipalConfigurationProvider()

	if err != nil {
		loglib.Sugar.Error(err)
		return err
	}
	
	// provider := common.DefaultConfigProvider()

	client, err := objectstorage.NewObjectStorageClientWithConfigurationProvider(provider)
	client.SetRegion(string(action.SourceRegion))

	if err != nil {
		loglib.Sugar.Error(err)
		return err
	}

	for _, dest := range action.DestinationRegions {
		request := objectstorage.DeleteObjectRequest {
			NamespaceName: common.String(action.Namespace),
			BucketName: common.String(action.BucketName),
			ObjectName: common.String(action.ObjectName),
		}

		client.SetRegion(dest)

		_, err := client.DeleteObject(action.ctx, request)

		if err != nil {
			loglib.Sugar.Error(err)
			return err
		}
	}

	return nil
}