package cmd

import (
	"errors"
	"fmt"
	"github.com/spf13/cobra"
	"path"
	"strings"
)

func init() {
	rootCmd.AddCommand(projectCmd)
	projectCmd.AddCommand(projectListCmd)
	projectCmd.AddCommand(projectGetOwnerCmd)
	projectCmd.AddCommand(projectUpdateSvcAccount)
	projectCmd.AddCommand(projectDeleteCmd)
	projectCmd.AddCommand(projectAddCmd)

	projectListCmd.Flags().StringP("owner", "o", "", "filter by owner account")
}

var projectCmd = &cobra.Command{
	Use:   "project",
	Short: "Project Spaces",
}

var projectAddCmd = &cobra.Command{
	Use:   "add <project-name> <svc-account>",
	Short: "Adds a new project (in db only)",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 2 {
			exit(cmd)
		}

		name := strings.TrimSpace(args[0])
		owner := strings.TrimSpace(args[1])

		if name == "" || owner == "" {
			err := errors.New("project name or owner is empty")
			er(err)
		}

		if err := addProject(name, owner); err != nil {
			er(err)
		}
	},
}

var projectDeleteCmd = &cobra.Command{
	Use:   "delete <project-name>",
	Short: "Deletes a project (in db only)",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			exit(cmd)
		}

		projectNameOrPath := strings.TrimSpace(args[0])

		project, err := getProject(projectNameOrPath)
		if err != nil {
			er(err)
		}

		if err := deleteProject(project); err != nil {
			er(err)
		}
	},
}

var projectUpdateSvcAccount = &cobra.Command{
	Use:   "update-svc-account <project-name> <svc-account>",
	Short: "Update the ownership of a project space (in db only)",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 2 {
			exit(cmd)
		}

		projectNameOrPath := strings.TrimSpace(args[0])
		owner := strings.TrimSpace(args[1])

		project, err := getProject(projectNameOrPath)
		if err != nil {
			er(err)
		}

		if err := updateProjectServiceAccount(project, owner); err != nil {
			er(err)
		}
	},
}

var projectListCmd = &cobra.Command{
	Use:   "list",
	Short: "List all project spaces",
	Run: func(cmd *cobra.Command, args []string) {
		owner, _ := cmd.Flags().GetString("owner")
		projects := getProjectSpaces(owner)

		cols := []string{"Name", "RelativePath", "Owner"}
		rows := [][]string{}
		for i := range projects {
			rows = append(rows, []string{projects[i].name, projects[i].rel, projects[i].owner})
		}
		pretty(cols, rows)
	},
}

var projectGetOwnerCmd = &cobra.Command{
	Use:   "getowner <project name or path>",
	Short: "Gets the owner of a project space",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			exit(cmd)
		}

		nameOrPath := strings.TrimSpace(args[0])

		owner, err := getProjectOwner(nameOrPath)
		if err != nil {
			er(err)
		}

		fmt.Println(owner)
	},
}

var addProject = func(name, owner string) error {
	db := getDB()

	if name == "" {
		panic("adding a new project: project  name is empty")
	}

	relpath := path.Join(string(name[0]), name)

	stmtString := "INSERT INTO cernbox_project_mapping(project_name, eos_relative_path, project_owner) VALUES(?,?,?)"
	stmt, err := db.Prepare(stmtString)
	if err != nil {
		return err
	}

	_, err = stmt.Exec(name, relpath, owner)
	if err != nil {
		return err
	}
	return nil

}

func deleteProject(project *projectSpace) error {
	db := getDB()

	// ensure name is not empty
	if project.name == "" {
		panic(fmt.Sprintf("project name is empty:%+v", project))
	}

	stmtString := "DELETE FROM cernbox_project_mapping WHERE project_name=?"
	stmt, err := db.Prepare(stmtString)
	if err != nil {
		return err
	}

	_, err = stmt.Exec(project.name)
	if err != nil {
		return err
	}
	return nil
}

func updateProjectServiceAccount(project *projectSpace, newOwner string) error {
	db := getDB()

	stmtString := "UPDATE cernbox_project_mapping SET project_owner=? WHERE project_name=?"
	stmt, err := db.Prepare(stmtString)
	if err != nil {
		return err
	}

	_, err = stmt.Exec(newOwner, project.name)
	if err != nil {
		return err
	}
	return nil
}

func getProjectSpaces(ownerFilter string) (projects []*projectSpace) {
	db := getDB()

	query := "SELECT project_name, eos_relative_path, project_owner FROM cernbox_project_mapping"
	rows, err := db.Query(query)
	if err != nil {
		er(err)
	}
	defer rows.Close()

	var name, relpath, owner string
	for rows.Next() {
		err := rows.Scan(&name, &relpath, &owner)
		if err != nil {
			er(err)
		}

		if ownerFilter == "" || ownerFilter == owner {
			proj := &projectSpace{name: name, rel: relpath, owner: owner}
			projects = append(projects, proj)
		}
	}

	err = rows.Err()
	if err != nil {
		er(err)
	}

	return
}

func getProjectOwner(nameOrPath string) (string, error) {
	relpath := getProjectRelPath(nameOrPath)
	projects := getProjectSpaces("")
	for i := range projects {
		if projects[i].rel == relpath {
			return projects[i].owner, nil
		}

		// give it a try without letter prefix, historically
		// there will be projects like "cernbox" or "ski club" under /eos/project
		base := path.Base(nameOrPath)
		if projects[i].rel == base {
			return projects[i].owner, nil
		}
	}

	return "", errors.New("error finding project owner for relative path: " + relpath)
}

func getProject(nameOrPath string) (*projectSpace, error) {
	relpath := getProjectRelPath(nameOrPath)
	projects := getProjectSpaces("")
	for i := range projects {
		if projects[i].rel == relpath {
			return projects[i], nil
		}

		// give it a try without letter prefix, historically
		// there will be projects like "cernbox" or "ski club" under /eos/project
		base := path.Base(nameOrPath)
		if projects[i].rel == base {
			return projects[i], nil
		}
	}
	return nil, errors.New("not found")
}

// name = cernbox
// path = /eos/project/cernbox/
// path = /eos/project/c/cernbox/
// path = c/cernbox/
// returns relative path: c/cernbox
func getProjectRelPath(nameOrPath string) string {
	base := path.Base(nameOrPath)
	return fmt.Sprintf("%s/%s", string(base[0]), base)
}

type projectSpace struct{ name, rel, owner string }
