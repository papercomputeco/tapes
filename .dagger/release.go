package main

import (
	"context"
	"fmt"
	"path"

	"dagger/tapes/internal/dagger"
)

type uploadOpts struct {
	// Directory containing build artifacts to upload
	artifacts *dagger.Directory

	// Path prefix in the bucket (e.g., "v1.0.0" or "nightly/2024-01-15")
	prefix string

	// Bucket endpoint URL
	endpoint *dagger.Secret

	// Bucket name
	bucket *dagger.Secret

	// Bucket access key ID
	accessKeyId *dagger.Secret

	// Bucket secret access key
	secretAccessKey *dagger.Secret
}

// Upload artifacts to bucket under the specified path prefix
func (t *Tapes) upload(
	ctx context.Context,
	opts *uploadOpts,
) error {
	bucketName, err := opts.bucket.Plaintext(ctx)
	if err != nil {
		return fmt.Errorf("failed to get bucket name: %w", err)
	}

	endpointUrl, err := opts.endpoint.Plaintext(ctx)
	if err != nil {
		return fmt.Errorf("failed to get endpoint: %w", err)
	}

	destination := fmt.Sprintf("s3://%s", path.Join(bucketName, opts.prefix))

	// Use AWS CLI container for S3-compatible uploads
	awsCli := dag.Container().
		From("amazon/aws-cli:latest").
		WithSecretVariable("AWS_ACCESS_KEY_ID", opts.accessKeyId).
		WithSecretVariable("AWS_SECRET_ACCESS_KEY", opts.secretAccessKey).
		WithEnvVariable("AWS_DEFAULT_REGION", "auto").
		WithDirectory("/artifacts", opts.artifacts).
		WithWorkdir("/artifacts")

	// Sync the artifacts directory to bucket
	_, err = awsCli.
		WithExec([]string{
			"aws", "s3", "sync", ".",
			destination,
			"--endpoint-url", endpointUrl,
		}).
		Sync(ctx)

	if err != nil {
		return fmt.Errorf("failed to upload artifacts: %w", err)
	}

	return nil
}

// Release builds release binaries and uploads to bucket
func (t *Tapes) ReleaseLatest(
	ctx context.Context,

	// Version string (e.g., "v1.0.0")
	version string,

	// Git commit SHA
	commit string,

	// Bucket endpoint URL
	endpoint *dagger.Secret,

	// Bucket name
	bucket *dagger.Secret,

	// Bucket access key ID
	accessKeyId *dagger.Secret,

	// Bucket secret access key
	secretAccessKey *dagger.Secret,
) (*dagger.Directory, error) {
	artifacts := t.BuildRelease(ctx, version, commit)
	err := t.upload(
		ctx,
		&uploadOpts{
			artifacts:       artifacts,
			prefix:          version,
			endpoint:        endpoint,
			bucket:          bucket,
			accessKeyId:     accessKeyId,
			secretAccessKey: secretAccessKey,
		},
	)

	if err != nil {
		return artifacts, fmt.Errorf("could not upload versioned release artifacts: %w", err)
	}

	err = t.upload(
		ctx,
		&uploadOpts{
			artifacts:       artifacts,
			prefix:          "latest",
			endpoint:        endpoint,
			bucket:          bucket,
			accessKeyId:     accessKeyId,
			secretAccessKey: secretAccessKey,
		},
	)

	if err != nil {
		return artifacts, fmt.Errorf("could not upload latest release artifacts: %w", err)
	}

	return artifacts, nil
}

// Nightly builds and uploads nightly artifacts
func (t *Tapes) Nightly(
	ctx context.Context,

	// Git commit SHA
	commit string,

	// Bucket endpoint URL
	endpoint *dagger.Secret,

	// Bucket name
	bucket *dagger.Secret,

	// Bucket access key ID
	accessKeyId *dagger.Secret,

	// Bucket secret access key
	secretAccessKey *dagger.Secret,
) (*dagger.Directory, error) {
	prefix := "nightly"
	artifacts := t.BuildRelease(ctx, prefix, commit)
	err := t.upload(
		ctx,
		&uploadOpts{
			artifacts:       artifacts,
			prefix:          prefix,
			endpoint:        endpoint,
			bucket:          bucket,
			accessKeyId:     accessKeyId,
			secretAccessKey: secretAccessKey,
		},
	)
	return artifacts, err
}

// UploadInstallSh uploads the install.sh script to the artifacts bucket
func (t *Tapes) UploadInstallSh(
	ctx context.Context,

	// Bucket endpoint URL
	endpoint *dagger.Secret,

	// Bucket name
	bucket *dagger.Secret,

	// Bucket access key ID
	accessKeyId *dagger.Secret,

	// Bucket secret access key
	secretAccessKey *dagger.Secret,
) error {
	installDir := dag.
		Directory().
		WithFile("install", t.Source.File("install.sh"))

	return t.upload(
		ctx,
		&uploadOpts{
			artifacts:       installDir,
			prefix:          "",
			endpoint:        endpoint,
			bucket:          bucket,
			accessKeyId:     accessKeyId,
			secretAccessKey: secretAccessKey,
		},
	)
}
