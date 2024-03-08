package objectstore

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/pkg/errors"
)

type StoreClient struct {
	*minio.Client
	Bucket string
}

func NewClientWithBucket(endpoint string, accessKeyID string, secretAccessKey string, bucket string) (StoreClient, error) {
	useSSL := true

	// Initialize minio client object.
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		return StoreClient{}, fmt.Errorf("NewOSWithBucket: %v", err)
	}

	var storeclient = StoreClient{
		minioClient,
		bucket,
	}
	return storeclient, nil

}

func (s *StoreClient) Download(object string, destdir string, filename string) error {

	err := os.MkdirAll(destdir, 0755)
	if err != nil {
		return err
	}

	err = s.FGetObject(context.Background(), s.Bucket, object, destdir+"/"+filename, minio.GetObjectOptions{})
	return err

}

func (s *StoreClient) ListObjs(prefix string) <-chan minio.ObjectInfo {

	objectCh := s.ListObjects(context.Background(), s.Bucket, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	})

	/*
		for object := range objectCh {
			if object.Err != nil {
				fmt.Println(object.Err)

			}
			fmt.Println(object.Key)
		}
	*/
	return objectCh
}

func (s *StoreClient) PutFile(path string, name string, options minio.PutObjectOptions) error {
	object, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("PutFile(): %v", err)
	}
	defer object.Close()
	objectStat, err := object.Stat()
	if err != nil {
		return fmt.Errorf("PutFile(): %v", err)
	}
	//minio.PutObjectOptions{ContentType: contentType, UserMetadata: metaData}
	//var metaData = map[string]string{}
	// "application/octet-stream"
	_, err = s.PutObject(context.Background(), s.Bucket, name, object, objectStat.Size(), options)
	if err != nil {
		return fmt.Errorf("PutFile(): %v", err)
	}
	return nil
}

var (
	doesNotExist = "The specified key does not exist."
)

func (s *StoreClient) PutS3ObjectBytes(objectName string, bytebufr []byte, options minio.PutObjectOptions) error {
	reader := bytes.NewReader(bytebufr)

	_, err := s.PutObject(context.Background(), s.Bucket, objectName, reader, reader.Size(), options)
	if err != nil {
		return fmt.Errorf("PutS3ObjectBytes(): %v", err)
	}
	return nil
}

func (s *StoreClient) GetS3ObjectBytes(objectName string) ([]byte, error) {

	/*
		objectInfo, err := s.StatObject(context.Background(), s.Bucket, objectName, minio.StatObjectOptions{})
		if err != nil {
			return nil, fmt.Errorf("%v", err)
		}
		log.Printf("Objectinfo:%+v", objectInfo)
	*/
	obj, err := s.GetObject(context.Background(), s.Bucket, objectName, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("%v", err)
	}
	defer obj.Close()

	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(obj)
	return buf.Bytes(), err

}

func (s *StoreClient) ObjectExists(name string) (bool, error) {
	_, err := s.StatObject(context.Background(), s.Bucket, name, minio.StatObjectOptions{})
	if err != nil {
		switch err.Error() {
		case doesNotExist:
			return false, nil
		default:
			return false, errors.Wrap(err, "error stating object")
		}
	}
	return true, nil
}

func (s *StoreClient) DownloadObj(objectName string, outPath string) error {

	obj, err := s.GetObject(context.Background(), s.Bucket, objectName, minio.GetObjectOptions{})
	if err != nil {
		return fmt.Errorf("DownloadObj.GetObject(): %v", err)
	}
	defer obj.Close()

	localFile, err := os.Create(outPath)
	if err != nil {
		return fmt.Errorf("DownloadObj.Create(): %v", err)
	}
	defer localFile.Close()

	_, err = io.Copy(localFile, obj)
	if err != nil {
		return fmt.Errorf("DownloadObj.Copy(): %v", err)
	}
	fi, err := localFile.Stat()
	if err != nil {
		return fmt.Errorf("DownloadObj.Stat(): %v", err)
	}
	if fi.Size() == 0 {
		return fmt.Errorf("DownloadObj file is zero bytes")
	}

	fmt.Printf("The file is %d bytes long", fi.Size())
	return nil

}

func (s *StoreClient) Object2Tempfile(objectName string) (string, error) {

	f, err := os.CreateTemp("/tmp", "object-")
	if err != nil {

		return "", fmt.Errorf("Object2Tempfile.CreateTem(): %v", err)
	}

	obj, err := s.GetObject(context.Background(), s.Bucket, objectName, minio.GetObjectOptions{})
	if err != nil {
		return "", fmt.Errorf("Object2Tempfile.GetObject(): %v", err)
	}
	defer obj.Close()

	localFile, err := os.Create(f.Name())
	if err != nil {
		return "", fmt.Errorf("Object2Tempfile.Create(): %v", err)
	}
	defer localFile.Close()

	if _, err = io.Copy(localFile, obj); err != nil {
		return "", fmt.Errorf("Object2Tempfile.Copy(): %v", err)
	}

	return f.Name(), nil

}

func (s StoreClient) Object2Filehandle(object string) {

}
