/*
Copyright 2025 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package conversion

import (
	"context"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

func MustNewHubSpokeConverter[hubObject runtime.Object](scheme *runtime.Scheme, hub runtime.Object, spokeConverter ...SpokeConverter[hubObject]) Converter {
	c, err := NewHubSpokeConverter(scheme, hub, spokeConverter...)
	if err != nil {
		panic(err)
	}
	return c
}

func NewHubSpokeConverter[hubObject runtime.Object](scheme *runtime.Scheme, hub runtime.Object, spokeConverter ...SpokeConverter[hubObject]) (Converter, error) {
	hubGVK, err := apiutil.GVKForObject(hub, scheme)
	if err != nil {
		return nil, fmt.Errorf("failed to create hub spoke converter: %w", err)
	}
	allGVKs, err := objectGVKs(scheme, hub)
	if err != nil {
		return nil, fmt.Errorf("failed to create hub spoke converter for %s: %w", hubGVK.Kind, err)
	}
	spokeVersions := sets.New[string]()
	for _, gvk := range allGVKs {
		if gvk != hubGVK {
			spokeVersions.Insert(gvk.Version)
		}
	}

	c := &hubSpokeConverter[hubObject]{
		scheme:              scheme,
		hubGVK:              hubGVK,
		spokeConverterByGVK: map[schema.GroupVersionKind]SpokeConverter[hubObject]{},
	}

	spokeConverterVersions := sets.New[string]()
	for _, sc := range spokeConverter {
		spokeGVK, err := apiutil.GVKForObject(sc.GetSpoke(), scheme)
		if err != nil {
			return nil, err
		}
		if hubGVK.GroupKind() != spokeGVK.GroupKind() {
			return nil, fmt.Errorf("failed to create hub spoke converter for %s: "+
				"spoke converter GroupKind %s does not match hub GroupKind %s",
				hubGVK.Kind, spokeGVK.GroupKind(), hubGVK.GroupKind())
		}

		if _, ok := c.spokeConverterByGVK[spokeGVK]; ok {
			return nil, fmt.Errorf("failed to create hub spoke converter for %s: "+
				"duplicate spoke converter for version %s",
				hubGVK.Kind, spokeGVK.Version)
		}
		c.spokeConverterByGVK[spokeGVK] = sc
		spokeConverterVersions.Insert(spokeGVK.Version)
	}

	if !spokeConverterVersions.Equal(spokeVersions) {
		return nil, fmt.Errorf("failed to create hub spoke converter for %s: "+
			"expected spoke converter for %s got spoke converter for %s",
			hubGVK.Kind, strings.Join(spokeVersions.UnsortedList(), ","), strings.Join(spokeConverterVersions.UnsortedList(), ","))
	}

	return c, nil
}

type hubSpokeConverter[hubObject runtime.Object] struct {
	scheme              *runtime.Scheme
	hubGVK              schema.GroupVersionKind
	spokeConverterByGVK map[schema.GroupVersionKind]SpokeConverter[hubObject]
}

func (c hubSpokeConverter[hubObject]) ConvertObject(ctx context.Context, src, dst runtime.Object) error {
	srcGVK := src.GetObjectKind().GroupVersionKind()
	dstGVK := dst.GetObjectKind().GroupVersionKind()

	srcIsHub := c.hubGVK == srcGVK
	dstIsHub := c.hubGVK == dstGVK
	_, srcIsConvertible := c.spokeConverterByGVK[srcGVK]
	_, dstIsConvertible := c.spokeConverterByGVK[dstGVK]

	switch {
	case srcIsHub && dstIsConvertible:
		return c.spokeConverterByGVK[dstGVK].ConvertHubToSpoke(ctx, src.(hubObject), dst)
	case dstIsHub && srcIsConvertible:
		return c.spokeConverterByGVK[srcGVK].ConvertSpokeToHub(ctx, src, dst.(hubObject))
	case srcIsConvertible && dstIsConvertible:
		hubGVK := c.hubGVK
		hub, err := c.scheme.New(hubGVK)
		if err != nil {
			return fmt.Errorf("failed to allocate an instance for gvk %v: %w", hubGVK, err)
		}
		if err := c.spokeConverterByGVK[srcGVK].ConvertSpokeToHub(ctx, src, hub.(hubObject)); err != nil {
			return fmt.Errorf("%T failed to convert to hub version %T : %w", src, hub, err)
		}
		if err := c.spokeConverterByGVK[dstGVK].ConvertHubToSpoke(ctx, hub.(hubObject), dst); err != nil {
			return fmt.Errorf("%T failed to convert from hub version %T : %w", dst, hub, err)
		}
	}
	return fmt.Errorf("%T is not convertible to %T", src, dst)
}

type SpokeConverter[hubObject runtime.Object] interface {
	GetSpoke() runtime.Object
	ConvertHubToSpoke(ctx context.Context, hub hubObject, spoke runtime.Object) error
	ConvertSpokeToHub(ctx context.Context, spoke runtime.Object, hub hubObject) error
}

func NewSpokeConverter[hubObject, spokeObject client.Object](
	spoke spokeObject,
	convertHubToSpokeFunc func(ctx context.Context, src hubObject, dst spokeObject) error,
	convertSpokeToHubFunc func(ctx context.Context, src spokeObject, dst hubObject) error,
) SpokeConverter[hubObject] {
	return &spokeConverter[hubObject, spokeObject]{
		spoke:                 spoke,
		convertSpokeToHubFunc: convertSpokeToHubFunc,
		convertHubToSpokeFunc: convertHubToSpokeFunc,
	}
}

type spokeConverter[hubObject, spokeObject runtime.Object] struct {
	spoke                 spokeObject
	convertHubToSpokeFunc func(ctx context.Context, src hubObject, dst spokeObject) error
	convertSpokeToHubFunc func(ctx context.Context, src spokeObject, dst hubObject) error
}

func (c spokeConverter[hubObject, spokeObject]) GetSpoke() runtime.Object {
	return c.spoke
}

func (c spokeConverter[hubObject, spokeObject]) ConvertHubToSpoke(ctx context.Context, hub hubObject, spoke runtime.Object) error {
	return c.convertHubToSpokeFunc(ctx, hub, spoke.(spokeObject))
}

func (c spokeConverter[hubObject, spokeObject]) ConvertSpokeToHub(ctx context.Context, spoke runtime.Object, hub hubObject) error {
	return c.convertSpokeToHubFunc(ctx, spoke.(spokeObject), hub)
}
