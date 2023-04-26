package containerd

import (
	"context"

	"github.com/containerd/containerd/images"
	"github.com/docker/distribution/reference"
	"github.com/docker/docker/api/types"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/sirupsen/logrus"
)

// ImageDelete deletes the image referenced by the given imageRef from this
// daemon. The given imageRef can be an image ID, ID prefix, or a repository
// reference (with an optional tag or digest, defaulting to the tag name
// "latest"). There is differing behavior depending on whether the given
// imageRef is a repository reference or not.
//
// If the given imageRef is a repository reference then that repository
// reference will be removed. However, if there exists any containers which
// were created using the same image reference then the repository reference
// cannot be removed unless either there are other repository references to the
// same image or force is true. Following removal of the repository reference,
// the referenced image itself will attempt to be deleted as described below
// but quietly, meaning any image delete conflicts will cause the image to not
// be deleted and the conflict will not be reported.
//
// There may be conflicts preventing deletion of an image and these conflicts
// are divided into two categories grouped by their severity:
//
// Hard Conflict:
//   - a pull or build using the image.
//   - any descendant image.
//   - any running container using the image.
//
// Soft Conflict:
//   - any stopped container using the image.
//   - any repository tag or digest references to the image.
//
// The image cannot be removed if there are any hard conflicts and can be
// removed if there are soft conflicts only if force is true.
//
// If prune is true, ancestor images will each attempt to be deleted quietly,
// meaning any delete conflicts will cause the image to not be deleted and the
// conflict will not be reported.
//
// TODO(thaJeztah): implement ImageDelete "force" options; see https://github.com/moby/moby/issues/43850
// TODO(thaJeztah): implement ImageDelete "prune" options; see https://github.com/moby/moby/issues/43849
// TODO(thaJeztah): image delete should send prometheus counters; see https://github.com/moby/moby/issues/45268
func (i *ImageService) ImageDelete(ctx context.Context, imageRef string, force, prune bool) ([]types.ImageDeleteResponseItem, error) {
	parsedRef, err := reference.ParseNormalizedNamed(imageRef)
	if err != nil {
		return nil, err
	}

	img, err := i.resolveImage(ctx, imageRef)
	if err != nil {
		return nil, err
	}

	possiblyDeletedConfigs := map[digest.Digest]struct{}{}
	if err := i.walkPresentChildren(ctx, img.Target, func(_ context.Context, d ocispec.Descriptor) {
		if images.IsConfigType(d.MediaType) {
			possiblyDeletedConfigs[d.Digest] = struct{}{}
		}
	}); err != nil {
		return nil, err
	}

	err = i.client.ImageService().Delete(ctx, img.Name, images.SynchronousDelete())
	if err != nil {
		return nil, err
	}

	// Workaround for: https://github.com/moby/buildkit/issues/3797
	if err := i.unleaseSnapshotsFromDeletedConfigs(context.Background(), possiblyDeletedConfigs); err != nil {
		logrus.WithError(err).Warn("failed to unlease snapshots")
	}

	imgID := string(img.Target.Digest)
	i.LogImageEvent(imgID, imgID, "untag")
	i.LogImageEvent(imgID, imgID, "delete")

	return []types.ImageDeleteResponseItem{{Untagged: reference.FamiliarString(parsedRef)}}, nil
}
