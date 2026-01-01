package main

import (
	"log"
	"os"
	"strings"
	"unicode"

	"encoding/json"

	"github.com/flosch/pongo2/v6"
)

// Root schema
type Schema struct {
	Filters     []Filter          `json:"filters"`
	TypeMap     map[string]string `json:"typeMap"`
	TypeAliases []TypeAlias       `json:"typeAliases"`
	Gen         []GenTemplate     `json:"gen"`
}

// Filters section
type Filter struct {
	Name            string            `json:"name"`
	Args            int               `json:"args"`
	Implementations map[string]string `json:"implementations"`
}

// Type aliases section
type TypeAlias struct {
	Name  string   `json:"name"`
	Items []string `json:"items"`
}

// Code generation templates
type GenTemplate struct {
	Name string            `json:"name"`
	Tpl  string            `json:"tpl"`
	Args map[string]string `json:"args"`
}

// Decode function
func DecodeSchemaJSON(data []byte) (*Schema, error) {
	var s Schema
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, err
	}
	return &s, nil
}

func init() {
	pongo2.RegisterFilter("capitalize", func(in, param *pongo2.Value) (out *pongo2.Value, err *pongo2.Error) {
		opLower := strings.ToLower(in.String())
		capitalizedOpName := strings.ToTitle(opLower[:1]) + opLower[1:]
		return pongo2.AsValue(capitalizedOpName), nil
	})
}

func Render(tpl string, data map[string]any) string {

	rendered, err := pongo2.RenderTemplateString(tpl, data)
	if err != nil {
		panic(err)
	}

	return rendered
}

func main() {

	readContents, readErr := os.ReadFile("./codegen.config.json")

	if readErr != nil {
		log.Printf("unable to read codegen.config.json, run a command inside ./ops directory")
	}

	parsed, parseErr := DecodeSchemaJSON(readContents)
	if parseErr != nil {
		log.Fatalf("error parsing gen config file: %v", parseErr)
	}

	filtersMapping := map[string]int{}

	for idx, item := range parsed.Filters {
		filtersMapping[item.Name] = idx
	}

	for _, itemToGen := range parsed.Gen {

		groupName := itemToGen.Name
		resultsMap := map[string]map[string]any{}

		nameTemplate := itemToGen.Tpl

		for _, operationObject := range parsed.Filters {
			opImpls := operationObject.Implementations

			for opImplType, specificImplFunc := range operationObject.Implementations {

				_ = specificImplFunc
				var typeAliasInfo TypeAlias

				if opImplType == "NumericTypes" {
					typeAliasInfo.Name = "NumericTypes"
				} else {
					for _, it := range parsed.TypeAliases {
						if it.Name == opImplType {
							typeAliasInfo = it
							break
						}
					}
				}

				templateData := map[string]any{
					"typeAlias":      typeAliasInfo,
					"filter":         operationObject,
					"filtersIndexes": filtersMapping,
				}

				funcName := Render(nameTemplate, templateData)

				_ = opImpls
				_ = typeAliasInfo

				resultsMap[funcName] = templateData

			}
		}

		for name, params := range resultsMap {

			params["config"] = parsed
			params["loop_unwrap_number"] = 2
			params["gen_range"] = func(hi int) []int {
				lo := 0
				s := make([]int, hi-lo)
				for i := range s {
					s[i] = i + lo
				}

				return s
			}

			params["to_json"] = func(v any) string {

				jb, _ := json.MarshalIndent(v, " ", " ")

				return string(jb)

			}

			pongo2.DefaultSet.CleanCache()
			renderContent, renderErr := pongo2.RenderTemplateFile("./codegen.templates/"+groupName+".go.twig", params)

			if renderErr != nil {
				log.Fatal(renderErr)
			} else {
				os.WriteFile("./generated/"+CamelToSnake(name)+".go", []byte(renderContent), 0755)
			}

		}

	}

}

func CamelToSnake(s string) string {
	var out []rune
	for i, r := range s {
		if unicode.IsUpper(r) {
			if i > 0 {
				out = append(out, '_')
			}
			out = append(out, unicode.ToLower(r))
		} else {
			out = append(out, r)
		}
	}
	return string(out)
}
