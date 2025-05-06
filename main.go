package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
)

var version = "dev"

// ANSI color codes
const (
	Reset     = "\033[0m"
	Bold      = "\033[1m"
	Underline = "\033[4m"
	Red       = "\033[31m"
	Green     = "\033[32m"
	Yellow    = "\033[33m"
	Blue      = "\033[34m"
	Magenta   = "\033[35m"
	Cyan      = "\033[36m"
	White     = "\033[37m"
	Black     = "\033[30m"
	BgRed     = "\033[41m"
	BgGreen   = "\033[42m"
	BgYellow  = "\033[43m"
	BgBlue    = "\033[44m"
	BgMagenta = "\033[45m"
	BgCyan    = "\033[46m"
	BgWhite   = "\033[47m"
)

// Constants for the Longhorn CRDs
const (
	longhornGroup     = "longhorn.io"
	longhornVersion   = "v1beta2"
	longhornNodes     = "nodes"
	longhornVolumes   = "volumes"
	longhornReplicas  = "replicas"
	longhornSettings  = "settings"
	longhornInstances = "instancemanagers"
	longhornEngines   = "engines"
)

// ByteSize represents a size in bytes
type ByteSize float64

// Size constants
const (
	_           = iota // ignore first value by assigning to blank identifier
	KB ByteSize = 1 << (10 * iota)
	MB
	GB
	TB
	PB
)

// String returns a human-readable representation of the byte size
func (b ByteSize) String() string {
	switch {
	case b >= PB:
		return fmt.Sprintf("%.2f PB", b/PB)
	case b >= TB:
		return fmt.Sprintf("%.2f TB", b/TB)
	case b >= GB:
		return fmt.Sprintf("%.2f GB", b/GB)
	case b >= MB:
		return fmt.Sprintf("%.2f MB", b/MB)
	case b >= KB:
		return fmt.Sprintf("%.2f KB", b/KB)
	default:
		return fmt.Sprintf("%.2f B", b)
	}
}

// DiskInfo stores information about a Longhorn disk
type DiskInfo struct {
	NodeName         string
	DiskName         string
	Path             string
	Tags             []string
	StorageMaximum   ByteSize
	StorageReserved  ByteSize
	StorageScheduled ByteSize
	StorageAvailable ByteSize
	Type             string
	PercentUsed      float64
}

// VolumeInfo stores information about a Longhorn volume
type VolumeInfo struct {
	Name            string
	Size            ByteSize
	ActualSize      ByteSize
	State           string
	Robustness      string
	Node            string
	ReplicaCount    int
	DesiredReplicas int
	Scheduled       bool
	Message         string
	DiskSelector    []string
	NodeSelector    []string
	Conditions      []ConditionInfo
	SafeToDelete    bool   // True if volume can be safely deleted
	DeleteReason    string // Reason why it's safe to delete
}

// ConditionInfo stores information about a condition
type ConditionInfo struct {
	Type      string
	Status    string
	Reason    string
	Message   string
	Timestamp string
}

// ReplicaInfo stores information about a Longhorn replica
type ReplicaInfo struct {
	Name       string
	VolumeName string
	InstanceID string
	NodeID     string
	DiskID     string
	DiskPath   string
	DataPath   string
	State      string
	FailedAt   string
	Size       ByteSize
	Mode       string
	Healthy    bool
}

// PersistentVolumeInfo stores information about a PV and its related resources
type PersistentVolumeInfo struct {
	Name             string
	Namespace        string
	StorageClass     string
	Size             string
	Status           string
	VolumeHandle     string
	PVCName          string
	PVCNamespace     string
	ConsumerPods     []PodInfo
	LonghornVolumeID string
}

// PodInfo stores basic information about a pod
type PodInfo struct {
	Name      string
	Namespace string
	Status    string
	NodeName  string
}

// Section holds configuration for a section header
type Section struct {
	Title       string
	Description string
	Color       string
}

var (
	// Define global color enablement
	useColors     = true
	compactOutput = false
)

func main() {
	// Parse command line flags
	var kubeconfig *string

	fmt.Println("LHMON4 Version:", version)

	if home := homedir.HomeDir(); home != "" {
		kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "absolute path to the kubeconfig file")
	} else {
		kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
	}
	namespace := flag.String("namespace", "longhorn-system", "namespace for Longhorn resources")
	nodeName := flag.String("node", "", "filter by node name (optional)")
	diskName := flag.String("disk", "", "filter by disk name (optional)")
	volumeName := flag.String("volume", "", "filter by volume name (optional)")
	diskTag := flag.String("disktag", "", "filter by disk tag (optional)")
	watch := flag.Bool("watch", false, "watch for changes")
	interval := flag.Int("interval", 5, "interval in seconds for watch mode")
	showReplicas := flag.Bool("replicas", true, "show detailed replica information")
	showRelationships := flag.Bool("relationships", true, "show Kubernetes resource relationships")
	verbose := flag.Bool("verbose", false, "show verbose error information")
	nocolor := flag.Bool("nocolor", false, "disable color output")
	compact := flag.Bool("compact", false, "use compact output format")
	flag.Parse()

	// Set global color setting
	useColors = !*nocolor
	compactOutput = *compact

	// Get Kubernetes config
	config, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		fmt.Printf("Error building kubeconfig: %v\n", err)
		os.Exit(1)
	}

	// Create dynamic client for CRDs
	dynClient, err := dynamic.NewForConfig(config)
	if err != nil {
		fmt.Printf("Error creating dynamic client: %v\n", err)
		os.Exit(1)
	}

	// Create standard client for core resources
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		fmt.Printf("Error creating Kubernetes client: %v\n", err)
		os.Exit(1)
	}

	// Define API resources
	nodesGVR := schema.GroupVersionResource{Group: longhornGroup, Version: longhornVersion, Resource: longhornNodes}
	volumesGVR := schema.GroupVersionResource{Group: longhornGroup, Version: longhornVersion, Resource: longhornVolumes}
	replicasGVR := schema.GroupVersionResource{Group: longhornGroup, Version: longhornVersion, Resource: longhornReplicas}

	// Run once or in watch mode
	if *watch {
		for {
			clearScreen()
			printHeader()

			// Get relationships first to determine safe-to-delete volumes
			pvInfoMap, err := getKubernetesRelationships(dynClient, clientset, *namespace, volumesGVR, *volumeName, *diskTag)
			if err != nil {
				fmt.Printf("Error getting relationships: %v\n", err)
			}

			err = printDiskInfo(dynClient, *namespace, nodesGVR, *nodeName, *diskName, *diskTag)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			}

			fmt.Println()
			err = printVolumeInfo(dynClient, *namespace, volumesGVR, *volumeName, *diskTag, *verbose, pvInfoMap)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			}

			if *showReplicas {
				fmt.Println()
				err = printReplicaInfo(dynClient, *namespace, replicasGVR, volumesGVR, *volumeName, *diskTag)
				if err != nil {
					fmt.Printf("Error: %v\n", err)
				}
			}

			if *showRelationships {
				fmt.Println()
				err = printKubernetesRelationships(dynClient, clientset, *namespace, volumesGVR, *volumeName, *diskTag)
				if err != nil {
					fmt.Printf("Error: %v\n", err)
				}
			}

			fmt.Printf("\n%sLast updated: %s%s\n", Bold, time.Now().Format("2006-01-02 15:04:05"), Reset)
			fmt.Printf("Watching for changes. Press Ctrl+C to exit...\n")
			time.Sleep(time.Duration(*interval) * time.Second)
		}
	} else {
		printHeader()

		// Get relationships first to determine safe-to-delete volumes
		pvInfoMap, err := getKubernetesRelationships(dynClient, clientset, *namespace, volumesGVR, *volumeName, *diskTag)
		if err != nil {
			fmt.Printf("Error getting relationships: %v\n", err)
		}

		err = printDiskInfo(dynClient, *namespace, nodesGVR, *nodeName, *diskName, *diskTag)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			os.Exit(1)
		}

		fmt.Println()
		err = printVolumeInfo(dynClient, *namespace, volumesGVR, *volumeName, *diskTag, *verbose, pvInfoMap)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
		}

		if *showReplicas {
			fmt.Println()
			err = printReplicaInfo(dynClient, *namespace, replicasGVR, volumesGVR, *volumeName, *diskTag)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			}
		}

		if *showRelationships {
			fmt.Println()
			err = printKubernetesRelationships(dynClient, clientset, *namespace, volumesGVR, *volumeName, *diskTag)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			}
		}

		// Print volumes safe to delete first - more important information
		printVolumeDeletionSummary(dynClient, *namespace, volumesGVR, pvInfoMap)

		fmt.Println("\nDisks with issues:")
		printProblematicDisks(dynClient, *namespace, nodesGVR)

		fmt.Println("\nVolumes with issues (detailed):")
		printDetailedVolumeIssues(dynClient, *namespace, volumesGVR, nodesGVR)

		fmt.Println("\nVolumes using disk tags:")
		printVolumesByDiskTag(dynClient, *namespace, volumesGVR)
	}
}

// printHeader prints a header for the output
func printHeader() {
	if useColors {
		fmt.Printf("%s%s═════════════════════════════════════════════════%s\n", Bold, Cyan, Reset)
		fmt.Printf("%s%s            LONGHORN STORAGE MONITOR            %s\n", Bold, Cyan, Reset)
		fmt.Printf("%s%s═════════════════════════════════════════════════%s\n", Bold, Cyan, Reset)
	} else {
		fmt.Println("═════════════════════════════════════════════════")
		fmt.Println("            LONGHORN STORAGE MONITOR            ")
		fmt.Println("═════════════════════════════════════════════════")
	}
	fmt.Println()
}

// clearScreen clears the terminal screen
func clearScreen() {
	fmt.Print("\033[H\033[2J")
}

// printSectionHeader prints a formatted section header
func printSectionHeader(section Section) {
	if useColors {
		color := section.Color
		if color == "" {
			color = Cyan
		}

		fmt.Printf("\n%s%s▌ %s %s\n", Bold, color, section.Title, Reset)
		if section.Description != "" {
			fmt.Printf("%s%s%s%s\n", Bold, color, section.Description, Reset)
		}
		fmt.Printf("%s%s%s\n", color, strings.Repeat("─", 50), Reset)
	} else {
		fmt.Printf("\n▌ %s\n", section.Title)
		if section.Description != "" {
			fmt.Printf("%s\n", section.Description)
		}
		fmt.Printf("%s\n", strings.Repeat("─", 50))
	}
}

// colorize adds ANSI color codes to text if colors are enabled
func colorize(text string, color string) string {
	if useColors && color != "" {
		return color + text + Reset
	}
	return text
}

// colorizeIf adds color only if the condition is true
// func colorizeIf(text string, color string, condition bool) string {
//	if condition && useColors && color != "" {
//		return color + text + Reset
//	}
//	return text
//}

// printDiskInfo prints disk information
func printDiskInfo(dynClient dynamic.Interface, namespace string, nodesGVR schema.GroupVersionResource, filterNode, filterDisk, filterTag string) error {
	// Get all nodes
	nodes, err := dynClient.Resource(nodesGVR).Namespace(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list Longhorn nodes: %v", err)
	}

	// Print section header
	printSectionHeader(Section{
		Title:       "DISK INFORMATION",
		Description: "Storage capacity and utilization of Longhorn disks",
		Color:       Blue,
	})

	// Collect all disk information
	var disks []DiskInfo
	for _, node := range nodes.Items {
		nodeName := node.GetName()

		// Skip if we're filtering by node and this isn't the right one
		if filterNode != "" && nodeName != filterNode {
			continue
		}

		// Get disk map from spec
		disksMap, found, err := unstructured.NestedMap(node.Object, "spec", "disks")
		if err != nil || !found || disksMap == nil {
			continue
		}

		// Get disk status map from status
		diskStatusMap, found, err := unstructured.NestedMap(node.Object, "status", "diskStatus")
		if err != nil || !found || diskStatusMap == nil {
			continue
		}

		// Process each disk
		for diskName, diskSpec := range disksMap {
			// Skip if we're filtering by disk and this isn't the right one
			if filterDisk != "" && diskName != filterDisk {
				continue
			}

			diskSpecMap, ok := diskSpec.(map[string]interface{})
			if !ok {
				continue
			}

			// Get disk path
			path, _ := diskSpecMap["path"].(string)

			// Get disk tags
			var tags []string
			tagsInterface, found := diskSpecMap["tags"]
			if found && tagsInterface != nil {
				tagsSlice, ok := tagsInterface.([]interface{})
				if ok {
					for _, t := range tagsSlice {
						if str, ok := t.(string); ok {
							tags = append(tags, str)
						}
					}
				}
			}

			// Skip if we're filtering by tag and this disk doesn't have that tag
			if filterTag != "" {
				hasTag := false
				for _, tag := range tags {
					if tag == filterTag {
						hasTag = true
						break
					}
				}
				if !hasTag {
					continue
				}
			}

			// Get disk type
			diskType, _ := diskSpecMap["diskType"].(string)

			// Get disk status
			diskStatusInterface, found := diskStatusMap[diskName]
			if !found {
				continue
			}

			diskStatus, ok := diskStatusInterface.(map[string]interface{})
			if !ok {
				continue
			}

			// Get storage metrics
			storageMaxFloat, _ := getFloat64(diskStatus, "storageMaximum")
			storageReservedFloat, _ := getFloat64(diskStatus, "storageReserved")
			storageScheduledFloat, _ := getFloat64(diskStatus, "storageScheduled")
			storageAvailableFloat, _ := getFloat64(diskStatus, "storageAvailable")

			storageMax := ByteSize(storageMaxFloat)
			storageReserved := ByteSize(storageReservedFloat)
			storageScheduled := ByteSize(storageScheduledFloat)
			storageAvailable := ByteSize(storageAvailableFloat)

			// Calculate percentage used
			percentUsed := 0.0
			if storageMax > 0 {
				percentUsed = 100.0 * (float64(storageMax-storageAvailable) / float64(storageMax))
			}

			// Create disk info
			disk := DiskInfo{
				NodeName:         nodeName,
				DiskName:         diskName,
				Path:             path,
				Tags:             tags,
				Type:             diskType,
				StorageMaximum:   storageMax,
				StorageReserved:  storageReserved,
				StorageScheduled: storageScheduled,
				StorageAvailable: storageAvailable,
				PercentUsed:      percentUsed,
			}

			disks = append(disks, disk)
		}
	}

	// Sort disks by node name and disk name
	sort.Slice(disks, func(i, j int) bool {
		if disks[i].NodeName == disks[j].NodeName {
			return disks[i].DiskName < disks[j].DiskName
		}
		return disks[i].NodeName < disks[j].NodeName
	})

	// Print disk information in a table
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', tabwriter.TabIndent)

	// Print header
	if useColors {
		fmt.Fprintf(w, "%s%sNODE\tDISK\tTAGS\tTYPE\tTOTAL\tAVAILABLE\tSCHEDULED\tUSED%%\tPATH%s\n", Bold, Yellow, Reset)
	} else {
		fmt.Fprintln(w, "NODE\tDISK\tTAGS\tTYPE\tTOTAL\tAVAILABLE\tSCHEDULED\tUSED%\tPATH")
	}

	fmt.Fprintln(w, "────\t────\t────\t────\t─────\t─────────\t─────────\t─────\t────")

	// Calculate the max total storage to find the expanded disks
	var maxStorage ByteSize = 0
	for _, disk := range disks {
		if disk.DiskName == "lv_01" && disk.StorageMaximum > maxStorage {
			maxStorage = disk.StorageMaximum
		}
	}

	// Print each disk with color coding for usage levels
	for _, disk := range disks {
		tagStr := "none"
		if len(disk.Tags) > 0 {
			tagStr = strings.Join(disk.Tags, ",")
		}

		// Color code the usage percentage
		usageStr := fmt.Sprintf("%.1f%%", disk.PercentUsed)
		usageColor := Green
		if disk.PercentUsed > 80 {
			usageColor = Red
		} else if disk.PercentUsed > 60 {
			usageColor = Yellow
		}

		// Highlight expanded disks (specifically lv_01 on k3sc003n02)
		nodeColor := ""
		diskColor := ""
		if disk.DiskName == "lv_01" && disk.StorageMaximum > ByteSize(float64(maxStorage)*0.9) {
			nodeColor = Green
			diskColor = Green + Bold
		}

		if useColors {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
				colorize(disk.NodeName, nodeColor),
				colorize(disk.DiskName, diskColor),
				colorize(tagStr, Cyan),
				disk.Type,
				colorize(disk.StorageMaximum.String(), Blue),
				colorize(disk.StorageAvailable.String(), Green),
				colorize(disk.StorageScheduled.String(), Yellow),
				colorize(usageStr, usageColor),
				disk.Path,
			)
		} else {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
				disk.NodeName,
				disk.DiskName,
				tagStr,
				disk.Type,
				disk.StorageMaximum,
				disk.StorageAvailable,
				disk.StorageScheduled,
				usageStr,
				disk.Path,
			)
		}
	}
	w.Flush()

	return nil
}

// printVolumeInfo prints volume information
func printVolumeInfo(dynClient dynamic.Interface, namespace string, volumesGVR schema.GroupVersionResource, filterVolume, filterTag string, verbose bool, pvInfoMap map[string]PersistentVolumeInfo) error {
	// Get all volumes
	volumes, err := dynClient.Resource(volumesGVR).Namespace(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list Longhorn volumes: %v", err)
	}

	// Print section header
	printSectionHeader(Section{
		Title:       "VOLUME INFORMATION",
		Description: "Longhorn volumes and their status",
		Color:       Magenta,
	})

	// Collect volume information
	var volumeInfos []VolumeInfo
	for _, volume := range volumes.Items {
		volumeName := volume.GetName()

		// Skip if we're filtering by volume name and this isn't the right one
		if filterVolume != "" && volumeName != filterVolume {
			continue
		}

		// Get disk selector
		diskSelector, found, _ := unstructured.NestedStringSlice(volume.Object, "spec", "diskSelector")

		// Skip if we're filtering by disk tag and this volume doesn't use that tag
		if filterTag != "" && (!found || !contains(diskSelector, filterTag)) {
			continue
		}

		// Get node selector
		nodeSelector, _, _ := unstructured.NestedStringSlice(volume.Object, "spec", "nodeSelector")

		// Get volume details
		sizeStr, _, _ := unstructured.NestedString(volume.Object, "spec", "size")
		size, _ := strconv.ParseFloat(sizeStr, 64)

		actualSizeFloat, _, _ := unstructured.NestedInt64(volume.Object, "status", "actualSize")

		state, _, _ := unstructured.NestedString(volume.Object, "status", "state")
		robustness, _, _ := unstructured.NestedString(volume.Object, "status", "robustness")
		nodeID, _, _ := unstructured.NestedString(volume.Object, "status", "currentNodeID")

		// Get replica count
		desiredReplicas, _, _ := unstructured.NestedInt64(volume.Object, "spec", "numberOfReplicas")

		// Determine if volume is scheduled
		scheduled := true
		message := ""

		// Get all conditions
		var conditions []ConditionInfo
		conditionsSlice, found, _ := unstructured.NestedSlice(volume.Object, "status", "conditions")
		if found {
			for _, c := range conditionsSlice {
				condition, ok := c.(map[string]interface{})
				if !ok {
					continue
				}

				condType, _ := condition["type"].(string)
				status, _ := condition["status"].(string)
				reason, _ := condition["reason"].(string)
				msg, _ := condition["message"].(string)
				ts, _ := condition["lastTransitionTime"].(string)

				// Check for scheduling issues
				if condType == "Scheduled" && status == "False" {
					scheduled = false
					message = msg
				}

				// Add to conditions
				conditions = append(conditions, ConditionInfo{
					Type:      condType,
					Status:    status,
					Reason:    reason,
					Message:   msg,
					Timestamp: ts,
				})
			}
		}

		// Count actual replicas
		// Count actual replicas - check both the map length and replica status
		replicaCount := 0
		activeReplicaCount := 0
		replicas, found, _ := unstructured.NestedMap(volume.Object, "status", "replicas")
		if found {
			// First count all replicas
			replicaCount = len(replicas)

			// Then count active replicas
			for _, r := range replicas {
				replica, ok := r.(map[string]interface{})
				if !ok {
					continue
				}

				// Check the mode - RW means active replica
				mode, modeFound, _ := unstructured.NestedString(replica, "mode")
				if modeFound && mode == "RW" {
					activeReplicaCount++
				}
			}
		}

		// If there are no direct replicas but the volume is attached and healthy,
		// we can assume it has at least one working replica
		if activeReplicaCount == 0 && state == "attached" && robustness == "healthy" {
			activeReplicaCount = 1
		}

		// Use the active replica count for display
		// replicaStatus := fmt.Sprintf("%d/%d", activeReplicaCount, desiredReplicas)

		// Check if this volume is safe to delete
		safeToDelete := false
		deleteReason := ""

		// Check PV status from the relationships
		if pvInfo, exists := pvInfoMap[volumeName]; exists {
			if pvInfo.Status == "Released" {
				safeToDelete = true
				deleteReason = "PV is in Released state and no longer used by any pod"
			} else if pvInfo.Status == "Failed" {
				safeToDelete = true
				deleteReason = "PV is in Failed state"
			}
		} else if state == "detached" {
			safeToDelete = true
			deleteReason = "Volume is detached and not bound to any PV"
		}

		// Create volume info
		volumeInfo := VolumeInfo{
			Name:            volumeName,
			Size:            ByteSize(size),
			ActualSize:      ByteSize(actualSizeFloat),
			State:           state,
			Robustness:      robustness,
			Node:            nodeID,
			ReplicaCount:    replicaCount,
			DesiredReplicas: int(desiredReplicas),
			Scheduled:       scheduled,
			Message:         message,
			DiskSelector:    diskSelector,
			NodeSelector:    nodeSelector,
			Conditions:      conditions,
			SafeToDelete:    safeToDelete,
			DeleteReason:    deleteReason,
		}

		volumeInfos = append(volumeInfos, volumeInfo)
	}

	// Sort volumes by name
	sort.Slice(volumeInfos, func(i, j int) bool {
		return volumeInfos[i].Name < volumeInfos[j].Name
	})

	// Print volume information in a table
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', tabwriter.TabIndent)

	// Print header
	if verbose {
		if useColors {
			fmt.Fprintf(w, "%s%sVOLUME\tSIZE\tSTATE\tROBUSTNESS\tNODE\tREPLICAS\tDISK SELECTOR\tSAFE TO DELETE%s\n", Bold, Yellow, Reset)
		} else {
			fmt.Fprintln(w, "VOLUME\tSIZE\tSTATE\tROBUSTNESS\tNODE\tREPLICAS\tDISK SELECTOR\tSAFE TO DELETE")
		}
	} else {
		if useColors {
			fmt.Fprintf(w, "%s%sVOLUME\tSIZE\tSTATE\tROBUSTNESS\tREPLICAS\tDISK SELECTOR\tSAFE TO DELETE%s\n", Bold, Yellow, Reset)
		} else {
			fmt.Fprintln(w, "VOLUME\tSIZE\tSTATE\tROBUSTNESS\tREPLICAS\tDISK SELECTOR\tSAFE TO DELETE")
		}
	}

	fmt.Fprintln(w, "──────\t────\t─────\t──────────\t────\t────────\t─────────────\t──────────────")

	for _, vol := range volumeInfos {
		replicaStatus := fmt.Sprintf("%d/%d", vol.ReplicaCount, vol.DesiredReplicas)

		diskSelectorStr := "none"
		if len(vol.DiskSelector) > 0 {
			diskSelectorStr = strings.Join(vol.DiskSelector, ",")
		}

		// Color code the different fields
		volNameColor := ""
		stateColor := Green
		robustnessColor := Green
		replicaColor := Green
		safeDeleteText := "No"
		safeDeleteColor := ""

		// Color coding based on state
		if vol.State == "detached" {
			stateColor = Yellow
		} else if vol.State == "error" {
			stateColor = Red
		}

		// Color coding based on robustness
		if vol.Robustness == "degraded" {
			robustnessColor = Yellow
		} else if vol.Robustness == "faulted" || vol.Robustness == "unknown" {
			robustnessColor = Red
		}

		// Color coding based on replicas
		if vol.ReplicaCount < vol.DesiredReplicas {
			replicaColor = Yellow
		} else if vol.ReplicaCount == 0 {
			replicaColor = Red
		}

		// Safe to delete highlighting
		if vol.SafeToDelete {
			safeDeleteText = "Yes - " + vol.DeleteReason
			safeDeleteColor = Green
			volNameColor = BgGreen + Black + Bold // Highlight volume name with green background
		}

		if verbose {
			if useColors {
				fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
					colorize(vol.Name, volNameColor),
					colorize(vol.Size.String(), Blue),
					colorize(vol.State, stateColor),
					colorize(vol.Robustness, robustnessColor),
					vol.Node,
					colorize(replicaStatus, replicaColor),
					colorize(diskSelectorStr, Cyan),
					colorize(safeDeleteText, safeDeleteColor),
				)
			} else {
				fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
					vol.Name,
					vol.Size,
					vol.State,
					vol.Robustness,
					vol.Node,
					replicaStatus,
					diskSelectorStr,
					safeDeleteText,
				)
			}
		} else {
			if useColors {
				fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
					colorize(vol.Name, volNameColor),
					colorize(vol.Size.String(), Blue),
					colorize(vol.State, stateColor),
					colorize(vol.Robustness, robustnessColor),
					colorize(replicaStatus, replicaColor),
					colorize(diskSelectorStr, Cyan),
					colorize(safeDeleteText, safeDeleteColor),
				)
			} else {
				fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
					vol.Name,
					vol.Size,
					vol.State,
					vol.Robustness,
					replicaStatus,
					diskSelectorStr,
					safeDeleteText,
				)
			}
		}
	}
	w.Flush()

	return nil
}

// printReplicaInfo prints detailed information about volume replicas
func printReplicaInfo(dynClient dynamic.Interface, namespace string, replicasGVR, volumesGVR schema.GroupVersionResource, filterVolume, filterTag string) error {
	// Get all replicas
	replicas, err := dynClient.Resource(replicasGVR).Namespace(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list Longhorn replicas: %v", err)
	}

	// Print section header
	printSectionHeader(Section{
		Title:       "REPLICA INFORMATION",
		Description: "Volume replicas and their placement",
		Color:       Cyan,
	})

	// If filtering by tag, we need to check which volumes use this tag
	volumesWithTag := make(map[string]bool)
	if filterTag != "" {
		volumes, err := dynClient.Resource(volumesGVR).Namespace(namespace).List(context.TODO(), metav1.ListOptions{})
		if err == nil {
			for _, volume := range volumes.Items {
				volumeName := volume.GetName()
				diskSelector, found, _ := unstructured.NestedStringSlice(volume.Object, "spec", "diskSelector")
				if found && contains(diskSelector, filterTag) {
					volumesWithTag[volumeName] = true
				}
			}
		}
	}

	// Create a map of volume name to a list of its replicas
	volumeReplicas := make(map[string][]ReplicaInfo)

	// Process each replica
	for _, replica := range replicas.Items {
		replicaName := replica.GetName()

		// Get replica info
		volumeName, _, _ := unstructured.NestedString(replica.Object, "spec", "volumeName")

		// Skip if we're filtering by volume and this isn't the right one
		if filterVolume != "" && volumeName != filterVolume {
			continue
		}

		// Skip if we're filtering by tag and this volume doesn't use that tag
		if filterTag != "" && !volumesWithTag[volumeName] {
			continue
		}

		instanceID, _, _ := unstructured.NestedString(replica.Object, "status", "instanceID")
		nodeID, _, _ := unstructured.NestedString(replica.Object, "spec", "nodeID")
		diskID, _, _ := unstructured.NestedString(replica.Object, "spec", "diskID")
		diskPath, _, _ := unstructured.NestedString(replica.Object, "spec", "diskPath")
		dataPath, _, _ := unstructured.NestedString(replica.Object, "status", "currentReplicaAddressMap", "dataPath")
		failedAt, _, _ := unstructured.NestedString(replica.Object, "status", "failedAt")

		sizeStr, _, _ := unstructured.NestedString(replica.Object, "spec", "size")
		size, _ := strconv.ParseFloat(sizeStr, 64)

		state, _, _ := unstructured.NestedString(replica.Object, "status", "state")
		mode, _, _ := unstructured.NestedString(replica.Object, "spec", "mode")

		// Determine if replica is healthy
		healthy := true
		if state == "ERR" || state == "FAILED" || failedAt != "" {
			healthy = false
		}

		// Create replica info
		replicaInfo := ReplicaInfo{
			Name:       replicaName,
			VolumeName: volumeName,
			InstanceID: instanceID,
			NodeID:     nodeID,
			DiskID:     diskID,
			DiskPath:   diskPath,
			DataPath:   dataPath,
			State:      state,
			FailedAt:   failedAt,
			Size:       ByteSize(size),
			Mode:       mode,
			Healthy:    healthy,
		}

		// Add to the map
		volumeReplicas[volumeName] = append(volumeReplicas[volumeName], replicaInfo)
	}

	// Sort and print replicas by volume
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', tabwriter.TabIndent)

	// Print header
	if useColors {
		fmt.Fprintf(w, "%s%sVOLUME\tREPLICA\tNODE\tDISK\tSTATE\tMODE\tHEALTHY\tSIZE%s\n", Bold, Yellow, Reset)
	} else {
		fmt.Fprintln(w, "VOLUME\tREPLICA\tNODE\tDISK\tSTATE\tMODE\tHEALTHY\tSIZE")
	}

	fmt.Fprintln(w, "──────\t───────\t────\t────\t─────\t────\t───────\t────")

	// Get sorted volume names
	volumeNames := make([]string, 0, len(volumeReplicas))
	for volumeName := range volumeReplicas {
		volumeNames = append(volumeNames, volumeName)
	}
	sort.Strings(volumeNames)

	// Print replicas for each volume
	for _, volumeName := range volumeNames {
		replicas := volumeReplicas[volumeName]

		// Sort replicas by node and name
		sort.Slice(replicas, func(i, j int) bool {
			if replicas[i].NodeID == replicas[j].NodeID {
				return replicas[i].Name < replicas[j].Name
			}
			return replicas[i].NodeID < replicas[j].NodeID
		})

		// Print replicas
		for _, replica := range replicas {
			healthStatus := "Yes"
			healthColor := Green
			if !replica.Healthy {
				healthStatus = "No"
				healthColor = Red
			}

			if useColors {
				fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
					colorize(replica.VolumeName, Blue),
					replica.Name,
					colorize(replica.NodeID, Cyan),
					replica.DiskID,
					replica.State,
					replica.Mode,
					colorize(healthStatus, healthColor),
					replica.Size,
				)
			} else {
				fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
					replica.VolumeName,
					replica.Name,
					replica.NodeID,
					replica.DiskID,
					replica.State,
					replica.Mode,
					healthStatus,
					replica.Size,
				)
			}
		}
	}
	w.Flush()

	return nil
}

// getKubernetesRelationships gets the relationships between Longhorn volumes, PVs, PVCs, and Pods
func getKubernetesRelationships(dynClient dynamic.Interface, clientset *kubernetes.Clientset, namespace string, volumesGVR schema.GroupVersionResource, filterVolume, filterTag string) (map[string]PersistentVolumeInfo, error) {
	// Get all Longhorn volumes
	volumes, err := dynClient.Resource(volumesGVR).Namespace(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to list Longhorn volumes: %v", err)
	}

	// Build a map of Longhorn volume ID to volume name
	longhornVolumes := make(map[string]string) // volumeID -> volumeName
	for _, volume := range volumes.Items {
		volumeName := volume.GetName()

		// Skip if we're filtering by volume name and this isn't the right one
		if filterVolume != "" && volumeName != filterVolume {
			continue
		}

		// Skip if we're filtering by disk tag and this volume doesn't use that tag
		if filterTag != "" {
			diskSelector, found, _ := unstructured.NestedStringSlice(volume.Object, "spec", "diskSelector")
			if !found || !contains(diskSelector, filterTag) {
				continue
			}
		}

		// Add to map
		longhornVolumes[volumeName] = volumeName
	}

	// Get all PVs
	pvs, err := clientset.CoreV1().PersistentVolumes().List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to list PersistentVolumes: %v", err)
	}

	// Build map of PV information
	pvInfoMap := make(map[string]PersistentVolumeInfo) // LH volume ID -> PVInfo
	for _, pv := range pvs.Items {
		// Skip if this PV doesn't use the CSI driver for Longhorn
		if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != "driver.longhorn.io" {
			continue
		}

		// Get the Longhorn volume ID from the volume handle
		longhornVolumeID := pv.Spec.CSI.VolumeHandle

		// Skip if we're filtering by volume
		if filterVolume != "" && longhornVolumeID != filterVolume {
			continue
		}

		// Skip if we're filtering by tag and this volume isn't in our map
		if filterTag != "" && longhornVolumes[longhornVolumeID] == "" {
			continue
		}

		// Create PV info
		pvInfo := PersistentVolumeInfo{
			Name:             pv.Name,
			StorageClass:     pv.Spec.StorageClassName,
			Size:             pv.Spec.Capacity.Storage().String(),
			Status:           string(pv.Status.Phase),
			VolumeHandle:     longhornVolumeID,
			LonghornVolumeID: longhornVolumeID,
		}

		// Set PVC info if bound
		if pv.Spec.ClaimRef != nil {
			pvInfo.PVCName = pv.Spec.ClaimRef.Name
			pvInfo.PVCNamespace = pv.Spec.ClaimRef.Namespace
		}

		// Add to map
		pvInfoMap[longhornVolumeID] = pvInfo
	}

	// Now get all pods and associate them with PVCs
	for volumeID, pvInfo := range pvInfoMap {
		// Skip if PVC info is not set
		if pvInfo.PVCName == "" || pvInfo.PVCNamespace == "" {
			continue
		}

		// Get all pods in the PVC's namespace
		pods, err := clientset.CoreV1().Pods(pvInfo.PVCNamespace).List(context.TODO(), metav1.ListOptions{})
		if err != nil {
			continue
		}

		// Find pods using this PVC
		for _, pod := range pods.Items {
			// Check each volume in the pod
			for _, volume := range pod.Spec.Volumes {
				// Check if this volume uses a PVC
				if volume.PersistentVolumeClaim != nil && volume.PersistentVolumeClaim.ClaimName == pvInfo.PVCName {
					// Add pod to the list
					podInfo := PodInfo{
						Name:      pod.Name,
						Namespace: pod.Namespace,
						Status:    string(pod.Status.Phase),
						NodeName:  pod.Spec.NodeName,
					}

					pvInfo.ConsumerPods = append(pvInfo.ConsumerPods, podInfo)

					// Update the map
					pvInfoMap[volumeID] = pvInfo
					break
				}
			}
		}
	}

	return pvInfoMap, nil
}

// printKubernetesRelationships prints the relationships between Longhorn volumes, PVs, PVCs, and Pods
func printKubernetesRelationships(dynClient dynamic.Interface, clientset *kubernetes.Clientset, namespace string, volumesGVR schema.GroupVersionResource, filterVolume, filterTag string) error {
	// Get relationships
	pvInfoMap, err := getKubernetesRelationships(dynClient, clientset, namespace, volumesGVR, filterVolume, filterTag)
	if err != nil {
		return err
	}

	// Print section header
	printSectionHeader(Section{
		Title:       "KUBERNETES RESOURCE RELATIONSHIPS",
		Description: "Mapping between Longhorn volumes, PVs, PVCs, and Pods",
		Color:       Green,
	})

	// Print the relationship information
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', tabwriter.TabIndent)

	// Print header
	if useColors {
		fmt.Fprintf(w, "%s%sLONGHORN VOLUME\tPV NAME\tPVC NAME\tPVC NAMESPACE\tSTORAGE CLASS\tSIZE\tSTATUS\tCONSUMER PODS%s\n", Bold, Yellow, Reset)
	} else {
		fmt.Fprintln(w, "LONGHORN VOLUME\tPV NAME\tPVC NAME\tPVC NAMESPACE\tSTORAGE CLASS\tSIZE\tSTATUS\tCONSUMER PODS")
	}

	fmt.Fprintln(w, "──────────────\t───────\t────────\t─────────────\t─────────────\t────\t──────\t────────────")

	// Create a sorted list of volume IDs for consistent output
	volumeIDs := make([]string, 0, len(pvInfoMap))
	for volumeID := range pvInfoMap {
		volumeIDs = append(volumeIDs, volumeID)
	}
	sort.Strings(volumeIDs)

	// Print each PV and its relationships
	for _, volumeID := range volumeIDs {
		pvInfo := pvInfoMap[volumeID]

		// Format consumer pods
		consumerPods := "none"
		if len(pvInfo.ConsumerPods) > 0 {
			podStrings := make([]string, 0, len(pvInfo.ConsumerPods))
			for _, pod := range pvInfo.ConsumerPods {
				podStrings = append(podStrings, fmt.Sprintf("%s (%s)", pod.Name, pod.Status))
			}
			consumerPods = strings.Join(podStrings, ", ")
		}

		// Format PVC info
		pvcInfo := "none"
		if pvInfo.PVCName != "" {
			pvcInfo = pvInfo.PVCName
		}

		pvcNamespace := "none"
		if pvInfo.PVCNamespace != "" {
			pvcNamespace = pvInfo.PVCNamespace
		}

		// Color coding based on status
		statusColor := Green
		if pvInfo.Status == "Released" {
			statusColor = Yellow
		} else if pvInfo.Status == "Failed" {
			statusColor = Red
		}

		// Determine row highlight color based on status
		volumeColor := ""
		if pvInfo.Status == "Released" || pvInfo.Status == "Failed" {
			volumeColor = BgGreen + Black + Bold
		}

		if useColors {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
				colorize(pvInfo.LonghornVolumeID, volumeColor),
				pvInfo.Name,
				colorize(pvcInfo, Blue),
				pvcNamespace,
				colorize(pvInfo.StorageClass, Cyan),
				pvInfo.Size,
				colorize(pvInfo.Status, statusColor),
				consumerPods,
			)
		} else {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
				pvInfo.LonghornVolumeID,
				pvInfo.Name,
				pvcInfo,
				pvcNamespace,
				pvInfo.StorageClass,
				pvInfo.Size,
				pvInfo.Status,
				consumerPods,
			)
		}
	}
	w.Flush()

	// If no relationships found, print a message
	if len(pvInfoMap) == 0 {
		fmt.Println("No Kubernetes resources found using Longhorn volumes")
	}

	return nil
}

// printVolumeDeletionSummary prints a summary of volumes that are safe to delete
func printVolumeDeletionSummary(dynClient dynamic.Interface, namespace string, volumesGVR schema.GroupVersionResource, pvInfoMap map[string]PersistentVolumeInfo) {
	// Find volumes that are safe to delete
	var safeDeletion []string
	var commands []string

	for volumeID, pvInfo := range pvInfoMap {
		if pvInfo.Status == "Released" || pvInfo.Status == "Failed" {
			safeDeletion = append(safeDeletion, volumeID)
			commands = append(commands, fmt.Sprintf("kubectl -n %s delete volumes.longhorn.io %s", namespace, volumeID))
		}
	}

	// Print section only if there are volumes to delete
	if len(safeDeletion) > 0 {
		printSectionHeader(Section{
			Title:       "VOLUMES SAFE TO DELETE",
			Description: "These volumes can be safely deleted",
			Color:       BgGreen + Black,
		})

		fmt.Println("The following volumes are safe to delete:")
		for _, vol := range safeDeletion {
			if useColors {
				fmt.Printf("  %s%s%s - %s\n", Green+Bold, vol, Reset, pvInfoMap[vol].Status)
			} else {
				fmt.Printf("  %s - %s\n", vol, pvInfoMap[vol].Status)
			}
		}

		fmt.Println("\nYou can delete them with the following commands:")
		for _, cmd := range commands {
			if useColors {
				fmt.Printf("  %s%s%s\n", Bold+Cyan, cmd, Reset)
			} else {
				fmt.Printf("  %s\n", cmd)
			}
		}
		fmt.Println()
	}
}

// printProblematicDisks prints disks with potential issues
func printProblematicDisks(dynClient dynamic.Interface, namespace string, nodesGVR schema.GroupVersionResource) {
	// Get all nodes
	nodes, err := dynClient.Resource(nodesGVR).Namespace(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		fmt.Printf("Error listing nodes: %v\n", err)
		return
	}

	// Print section header
	printSectionHeader(Section{
		Title:       "DISKS WITH ISSUES",
		Description: "Problems detected with Longhorn disks",
		Color:       Red,
	})

	// Setup tabwriter
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', tabwriter.TabIndent)

	// Print header
	if useColors {
		fmt.Fprintf(w, "%s%sNODE\tDISK\tISSUE%s\n", Bold, Yellow, Reset)
	} else {
		fmt.Fprintln(w, "NODE\tDISK\tISSUE")
	}

	fmt.Fprintln(w, "────\t────\t─────")

	foundIssues := false

	// Process each node
	for _, node := range nodes.Items {
		nodeName := node.GetName()

		// Get disk map from spec
		disksMap, found, err := unstructured.NestedMap(node.Object, "spec", "disks")
		if err != nil || !found {
			continue
		}

		// Get disk status map from status
		diskStatusMap, found, err := unstructured.NestedMap(node.Object, "status", "diskStatus")
		if err != nil || !found {
			continue
		}

		// Process each disk
		for diskName, diskSpec := range disksMap {
			diskSpecMap, ok := diskSpec.(map[string]interface{})
			if !ok {
				continue
			}

			// Check if disk has tags
			tags, found := diskSpecMap["tags"]
			if !found || tags == nil {
				if useColors {
					fmt.Fprintf(w, "%s\t%s\t%sNo tags defined%s\n", nodeName, diskName, Red, Reset)
				} else {
					fmt.Fprintf(w, "%s\t%s\tNo tags defined\n", nodeName, diskName)
				}
				foundIssues = true
				continue
			}

			// Check if disk has status
			_, found = diskStatusMap[diskName]
			if !found {
				if useColors {
					fmt.Fprintf(w, "%s\t%s\t%sNo disk status available%s\n", nodeName, diskName, Red, Reset)
				} else {
					fmt.Fprintf(w, "%s\t%s\tNo disk status available\n", nodeName, diskName)
				}
				foundIssues = true
				continue
			}

			// Check disk conditions for any issues
			conditions, found, _ := unstructured.NestedSlice(diskStatusMap, diskName, "conditions")
			if found {
				for _, c := range conditions {
					condition, ok := c.(map[string]interface{})
					if !ok {
						continue
					}

					condType, _ := condition["type"].(string)
					status, _ := condition["status"].(string)
					reason, _ := condition["reason"].(string)

					if status == "False" && condType != "" {
						if useColors {
							fmt.Fprintf(w, "%s\t%s\t%s%s: %s%s\n", nodeName, diskName, Red, condType, reason, Reset)
						} else {
							fmt.Fprintf(w, "%s\t%s\t%s: %s\n", nodeName, diskName, condType, reason)
						}
						foundIssues = true
					}
				}
			}
		}
	}

	if !foundIssues {
		fmt.Fprintln(w, "No disk issues found")
	}

	w.Flush()
}

func printDetailedVolumeIssues(dynClient dynamic.Interface, namespace string, volumesGVR, nodesGVR schema.GroupVersionResource) {
	// Get all volumes
	volumes, err := dynClient.Resource(volumesGVR).Namespace(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		fmt.Printf("Error listing volumes: %v\n", err)
		return
	}

	// Get all nodes for disk info
	nodes, err := dynClient.Resource(nodesGVR).Namespace(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		fmt.Printf("Error listing nodes: %v\n", err)
	}

	// Print section header
	printSectionHeader(Section{
		Title:       "VOLUMES WITH ISSUES",
		Description: "Detailed diagnosis and solutions",
		Color:       Red,
	})

	// Build disk info map for diagnostics
	diskInfoMap := make(map[string]map[string]DiskInfo) // node -> disk -> info
	if err == nil {
		for _, node := range nodes.Items {
			nodeName := node.GetName()
			diskInfoMap[nodeName] = make(map[string]DiskInfo)

			// Get disk map from spec
			disksMap, found, err := unstructured.NestedMap(node.Object, "spec", "disks")
			if err != nil || !found || disksMap == nil {
				continue
			}

			// Get disk status map from status
			diskStatusMap, found, err := unstructured.NestedMap(node.Object, "status", "diskStatus")
			if err != nil || !found || diskStatusMap == nil {
				continue
			}

			// Process each disk
			for diskName, diskSpec := range disksMap {
				diskSpecMap, ok := diskSpec.(map[string]interface{})
				if !ok {
					continue
				}

				// Get disk path
				path, _ := diskSpecMap["path"].(string)

				// Get disk tags
				var tags []string
				tagsInterface, found := diskSpecMap["tags"]
				if found && tagsInterface != nil {
					tagsSlice, ok := tagsInterface.([]interface{})
					if ok {
						for _, t := range tagsSlice {
							if str, ok := t.(string); ok {
								tags = append(tags, str)
							}
						}
					}
				}

				// Get disk type
				diskType, _ := diskSpecMap["diskType"].(string)

				// Get disk status
				diskStatusInterface, found := diskStatusMap[diskName]
				if !found {
					continue
				}

				diskStatus, ok := diskStatusInterface.(map[string]interface{})
				if !ok {
					continue
				}

				// Get storage metrics
				storageMaxFloat, _ := getFloat64(diskStatus, "storageMaximum")
				storageReservedFloat, _ := getFloat64(diskStatus, "storageReserved")
				storageScheduledFloat, _ := getFloat64(diskStatus, "storageScheduled")
				storageAvailableFloat, _ := getFloat64(diskStatus, "storageAvailable")

				storageMax := ByteSize(storageMaxFloat)
				storageReserved := ByteSize(storageReservedFloat)
				storageScheduled := ByteSize(storageScheduledFloat)
				storageAvailable := ByteSize(storageAvailableFloat)

				// Calculate percentage used
				percentUsed := 0.0
				if storageMax > 0 {
					percentUsed = 100.0 * (float64(storageMax-storageAvailable) / float64(storageMax))
				}

				// Create disk info
				disk := DiskInfo{
					NodeName:         nodeName,
					DiskName:         diskName,
					Path:             path,
					Tags:             tags,
					Type:             diskType,
					StorageMaximum:   storageMax,
					StorageReserved:  storageReserved,
					StorageScheduled: storageScheduled,
					StorageAvailable: storageAvailable,
					PercentUsed:      percentUsed,
				}

				diskInfoMap[nodeName][diskName] = disk
			}
		}
	}

	// Setup tabwriter
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', tabwriter.TabIndent)

	// Print header
	if useColors {
		fmt.Fprintf(w, "%s%sVOLUME\tSTATE\tROBUSTNESS\tREPLICAS\tISSUE\tPOSSIBLE SOLUTION%s\n", Bold, Yellow, Reset)
	} else {
		fmt.Fprintln(w, "VOLUME\tSTATE\tROBUSTNESS\tREPLICAS\tISSUE\tPOSSIBLE SOLUTION")
	}

	fmt.Fprintln(w, "──────\t─────\t──────────\t────────\t─────\t─────────────────")

	foundIssues := false

	// Process each volume
	for _, volume := range volumes.Items {
		volumeName := volume.GetName()

		state, _, _ := unstructured.NestedString(volume.Object, "status", "state")
		robustness, _, _ := unstructured.NestedString(volume.Object, "status", "robustness")

		// Get desired and actual replica counts
		desiredReplicas, _, _ := unstructured.NestedInt64(volume.Object, "spec", "numberOfReplicas")

		// Count actual replicas - check both replicas map and replica failures
		replicaCount := 0
		replicas, found, _ := unstructured.NestedMap(volume.Object, "status", "replicas")
		if found {
			replicaCount = len(replicas)

			// Check if any replicas are healthy
			for _, r := range replicas {
				replica, ok := r.(map[string]interface{})
				if !ok {
					continue
				}

				mode, found, _ := unstructured.NestedString(replica, "mode")
				if found && mode == "RW" {
					// hasHealthyReplicas := true
					break
				}
			}
		}

		replicaStatus := fmt.Sprintf("%d/%d", replicaCount, desiredReplicas)

		// Get disk and node selectors
		diskSelector, _, _ := unstructured.NestedStringSlice(volume.Object, "spec", "diskSelector")
		nodeSelector, _, _ := unstructured.NestedStringSlice(volume.Object, "spec", "nodeSelector")

		// Get volume size
		sizeStr, _, _ := unstructured.NestedString(volume.Object, "spec", "size")
		size, _ := strconv.ParseFloat(sizeStr, 64)
		volumeSize := ByteSize(size)

		// Color coding
		stateColor := Green
		robustnessColor := Green

		if state == "detached" {
			stateColor = Yellow
		} else if state == "error" {
			stateColor = Red
		}

		if robustness == "degraded" {
			robustnessColor = Yellow
		} else if robustness == "faulted" || robustness == "unknown" {
			robustnessColor = Red
		}

		// Check if this volume actually has issues
		hasIssue := false

		// Volumes with attached state but unhealthy robustness
		if state == "attached" && (robustness == "degraded" || robustness == "faulted" || robustness == "unknown") {
			hasIssue = true
		}

		// Detached or errored volumes
		if state == "detached" || state == "error" {
			hasIssue = true
		}

		// Explicit check for condition failures
		conditionFailure := false
		failedConditions := make([]ConditionInfo, 0)

		conditions, found, _ := unstructured.NestedSlice(volume.Object, "status", "conditions")
		if found {
			for _, c := range conditions {
				condition, ok := c.(map[string]interface{})
				if !ok {
					continue
				}

				condType, _ := condition["type"].(string)
				status, _ := condition["status"].(string)
				reason, _ := condition["reason"].(string)
				message, _ := condition["message"].(string)

				// Skip certain condition types that don't indicate problems
				if condType == "Restore" || condType == "WaitForBackingImage" {
					continue
				}

				if status == "False" && message != "" {
					conditionFailure = true
					failedConditions = append(failedConditions, ConditionInfo{
						Type:    condType,
						Status:  status,
						Reason:  reason,
						Message: message,
					})
				}
			}
		}

		if conditionFailure {
			hasIssue = true
		}

		// Only process volumes with actual issues
		if hasIssue {
			// Get issue details from conditions
			if len(failedConditions) > 0 {
				for _, cond := range failedConditions {
					// Perform diagnostics based on the issue type and add solutions
					solution := "Unknown issue, check Longhorn logs for more details"

					// Tag issues - check if any disk has the required tag
					if strings.Contains(cond.Message, "tags not fulfilled") || strings.Contains(cond.Message, "no disk matches requirements") {
						// Analyze available disks vs required tags
						availableDisks := 0
						availableSpace := ByteSize(0)
						requiredTags := make(map[string]bool)

						// Collect required tags
						for _, tag := range diskSelector {
							requiredTags[tag] = true
						}

						// Count disks with the required tags and their available space
						for _, disks := range diskInfoMap {
							for _, diskInfo := range disks {
								hasAllTags := true
								for tag := range requiredTags {
									if !contains(diskInfo.Tags, tag) {
										hasAllTags = false
										break
									}
								}

								if hasAllTags {
									availableDisks++
									availableSpace += diskInfo.StorageAvailable
								}
							}
						}

						// Generate solution based on findings
						if availableDisks == 0 {
							solution = fmt.Sprintf("No disks found with required tags: %s. Add these tags to appropriate disks or modify volume to use different tags.", strings.Join(diskSelector, ","))
						} else if availableSpace < volumeSize {
							solution = fmt.Sprintf("Insufficient space on disks with required tags. Available: %s, Required: %s. Extend disk space or reduce volume size.", availableSpace, volumeSize)
						} else {
							solution = fmt.Sprintf("Disk tags match but scheduling failed. Check node conditions and Longhorn manager logs.")
						}
					} else if strings.Contains(cond.Message, "insufficient storage") {
						// Storage space issues
						solution = fmt.Sprintf("Not enough storage space available for volume size %s. Extend storage on disks with appropriate tags or reduce volume size.", volumeSize)
					} else if strings.Contains(cond.Message, "specified node tag") || strings.Contains(cond.Message, "node tag") {
						// Node tag issues
						solution = fmt.Sprintf("Node selector tags not fulfilled: %s. Add these tags to appropriate nodes or modify volume to use different node selector.", strings.Join(nodeSelector, ","))
					} else if strings.Contains(cond.Message, "error creating") || strings.Contains(cond.Message, "create volume error") {
						// Volume creation issues
						solution = "Error during volume creation. Check Longhorn manager logs for details. Try deleting and recreating the volume."
					} else if strings.Contains(cond.Message, "error attaching") {
						// Volume attachment issues
						solution = "Error attaching volume. Check that the node has access to the storage. Try restarting the Longhorn manager on the node."
					}

					issueText := fmt.Sprintf("%s: %s", cond.Type, cond.Message)
					if useColors {
						fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n",
							volumeName,
							colorize(state, stateColor),
							colorize(robustness, robustnessColor),
							replicaStatus,
							colorize(issueText, Red),
							colorize(solution, Yellow),
						)
					} else {
						fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n",
							volumeName,
							state,
							robustness,
							replicaStatus,
							issueText,
							solution,
						)
					}
					foundIssues = true
				}
			} else {
				// Handle volumes with state/robustness issues but no explicit condition failure
				solution := "Unknown issue, check Longhorn logs for more details"
				issueText := "Volume has issues but no specific condition found"

				if state == "detached" {
					solution = "Volume is detached. Attach the volume to a workload or delete it if no longer needed."
				} else if robustness == "unknown" {
					solution = "Volume robustness is unknown. This may be a transient state. If it persists, try restarting the Longhorn manager."
				} else if state == "error" {
					solution = "Volume is in error state. Check Longhorn manager logs for details."
				}

				if useColors {
					fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n",
						volumeName,
						colorize(state, stateColor),
						colorize(robustness, robustnessColor),
						replicaStatus,
						colorize(issueText, Red),
						colorize(solution, Yellow),
					)
				} else {
					fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n",
						volumeName,
						state,
						robustness,
						replicaStatus,
						issueText,
						solution,
					)
				}
				foundIssues = true
			}
		}
	}

	if !foundIssues {
		fmt.Fprintln(w, "No volume issues found")
	}

	w.Flush()
}

// printVolumesByDiskTag prints volumes that use specific disk tags
func printVolumesByDiskTag(dynClient dynamic.Interface, namespace string, volumesGVR schema.GroupVersionResource) {
	// Get all volumes
	volumes, err := dynClient.Resource(volumesGVR).Namespace(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		fmt.Printf("Error listing volumes: %v\n", err)
		return
	}

	// Print section header
	printSectionHeader(Section{
		Title:       "VOLUMES BY DISK TAG",
		Description: "Volumes grouped by the disk tags they use",
		Color:       Cyan,
	})

	// Setup tabwriter
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', tabwriter.TabIndent)

	// Print header
	if useColors {
		fmt.Fprintf(w, "%s%sVOLUME\tDISK SELECTOR\tSTATE\tROBUSTNESS\tREPLICAS\tSIZE%s\n", Bold, Yellow, Reset)
	} else {
		fmt.Fprintln(w, "VOLUME\tDISK SELECTOR\tSTATE\tROBUSTNESS\tREPLICAS\tSIZE")
	}

	fmt.Fprintln(w, "──────\t─────────────\t─────\t──────────\t────────\t────")

	foundVolumes := false

	// Process each volume
	for _, volume := range volumes.Items {
		volumeName := volume.GetName()

		// Get disk selector
		diskSelector, found, _ := unstructured.NestedStringSlice(volume.Object, "spec", "diskSelector")
		if !found || len(diskSelector) == 0 {
			continue
		}

		state, _, _ := unstructured.NestedString(volume.Object, "status", "state")
		robustness, _, _ := unstructured.NestedString(volume.Object, "status", "robustness")

		sizeStr, _, _ := unstructured.NestedString(volume.Object, "spec", "size")
		size, _ := strconv.ParseFloat(sizeStr, 64)
		sizeBytes := ByteSize(size)

		// Get replica count
		desiredReplicas, _, _ := unstructured.NestedInt64(volume.Object, "spec", "numberOfReplicas")

		// Count actual replicas
		// Count actual replicas - check both the map length and replica status
		activeReplicaCount := 0
		replicas, found, _ := unstructured.NestedMap(volume.Object, "status", "replicas")
		if found {
			// First count all replicas
			// replicaCount = len(replicas)

			// Then count active replicas
			for _, r := range replicas {
				replica, ok := r.(map[string]interface{})
				if !ok {
					continue
				}

				// Check the mode - RW means active replica
				mode, modeFound, _ := unstructured.NestedString(replica, "mode")
				if modeFound && mode == "RW" {
					activeReplicaCount++
				}
			}
		}

		// If there are no direct replicas but the volume is attached and healthy,
		// we can assume it has at least one working replica
		if activeReplicaCount == 0 && state == "attached" && robustness == "healthy" {
			activeReplicaCount = 1
		}

		// Use the active replica count for display
		replicaStatus := fmt.Sprintf("%d/%d", activeReplicaCount, desiredReplicas)

		// Color coding
		stateColor := Green
		robustnessColor := Green

		if state == "detached" {
			stateColor = Yellow
		} else if state == "error" {
			stateColor = Red
		}

		if robustness == "degraded" {
			robustnessColor = Yellow
		} else if robustness == "faulted" || robustness == "unknown" {
			robustnessColor = Red
		}

		if useColors {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n",
				volumeName,
				colorize(strings.Join(diskSelector, ","), Cyan),
				colorize(state, stateColor),
				colorize(robustness, robustnessColor),
				replicaStatus,
				colorize(sizeBytes.String(), Blue),
			)
		} else {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n",
				volumeName,
				strings.Join(diskSelector, ","),
				state,
				robustness,
				replicaStatus,
				sizeBytes.String(),
			)
		}

		foundVolumes = true
	}

	if !foundVolumes {
		fmt.Fprintln(w, "No volumes using disk tags found")
	}

	w.Flush()
}

// getFloat64 extracts a float64 value from a map
func getFloat64(m map[string]interface{}, key string) (float64, bool) {
	v, found := m[key]
	if !found {
		return 0, false
	}

	switch value := v.(type) {
	case float64:
		return value, true
	case int:
		return float64(value), true
	case int64:
		return float64(value), true
	case string:
		f, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return 0, false
		}
		return f, true
	default:
		return 0, false
	}
}

// contains checks if a string slice contains a specific value
func contains(slice []string, value string) bool {
	for _, item := range slice {
		if item == value {
			return true
		}
	}
	return false
}
