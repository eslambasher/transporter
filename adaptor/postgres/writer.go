package postgres

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/compose/mejson"
	"github.com/compose/transporter/client"
	"github.com/compose/transporter/log"
	"github.com/compose/transporter/message"
	"github.com/compose/transporter/message/ops"
)

var _ client.Writer = &Writer{}

// Writer implements client.Writer for use with MongoDB
type Writer struct {
	writeMap map[ops.Op]func(message.Msg, *sql.DB) error
}

// UpsertValues is used to contruct the upsert statement
type UpsertValues struct {
	Inames []string
	Ikeys  []string
	Ckeys  []string
	Ukeys  []string
	Vals   []interface{}
	Pkeys  map[string]bool
}

func newWriter() *Writer {
	w := &Writer{}
	w.writeMap = map[ops.Op]func(message.Msg, *sql.DB) error{
		ops.Insert: insertMsg,
		ops.Update: updateMsg,
		ops.Delete: deleteMsg,
	}
	return w
}

func (w *Writer) Write(msg message.Msg) func(client.Session) (message.Msg, error) {
	return func(s client.Session) (message.Msg, error) {
		writeFunc, ok := w.writeMap[msg.OP()]
		if !ok {
			log.Infof("no function registered for operation, %s", msg.OP())
			if msg.Confirms() != nil {
				msg.Confirms() <- struct{}{}
			}
			return msg, nil
		}
		if err := writeFunc(msg, s.(*Session).pqSession); err != nil {
			return nil, err
		}
		if msg.Confirms() != nil {
			msg.Confirms() <- struct{}{}
		}
		return msg, nil
	}
}

func insertMsg(m message.Msg, s *sql.DB) error {
	/*log.With("table", m.Namespace()).Debugln("INSERT")
	var (
		keys         []string
		placeholders []string
		data         []interface{}
	)

	i := 1
	for key, value := range m.Data() {
		keys = append(keys, key)
		placeholders = append(placeholders, fmt.Sprintf("$%v", i))

		switch value.(type) {
		case map[string]interface{}, mejson.M, []map[string]interface{}, mejson.S:
			value, _ = json.Marshal(value)
		case []interface{}:
			value, _ = json.Marshal(value)
			value = string(value.([]byte))
			value = fmt.Sprintf("{%v}", value.(string)[1:len(value.(string))-1])
		}
		data = append(data, value)

		i = i + 1
	}

	query := fmt.Sprintf("INSERT INTO %v (%v) VALUES (%v);", m.Namespace(), strings.Join(keys, ", "), strings.Join(placeholders, ", "))
	_, err := s.Exec(query, data...)
	return err*/

	// fmt.Printf("Write INSERT to Postgres %v values %v\n", m.Namespace(), m.Data())
	// var (
	// 	keys         []string
	// 	placeholders []string
	// 	data         []interface{}
	// )

	// i := 1
	// for key, value := range m.Data() {
	// 	keys = append(keys, key)
	// 	placeholders = append(placeholders, fmt.Sprintf("$%v", i))

	// 	switch value.(type) {
	// 	case map[string]interface{}:
	// 		value, _ = json.Marshal(value)
	// 	case []interface{}:
	// 		value, _ = json.Marshal(value)
	// 	}
	// 	data = append(data, value)

	// 	i = i + 1
	// }

	st, error := GenerateUpsertStatement(m, s)

	if error != nil {
		return error
	}

	fmt.Printf("Write UPSERT BIT to Postgres %v\n", strings.Join(st.Ukeys, ", "))

	query := fmt.Sprintf("INSERT INTO %v (%v) VALUES (%v)", m.Namespace(), strings.Join(st.Inames, ", "), strings.Join(st.Ikeys, ", "))

	pkeys := reflect.ValueOf(st.Pkeys).MapKeys()
	strkeys := make([]string, len(pkeys))
	for i := 0; i < len(pkeys); i++ {
		strkeys[i] = pkeys[i].String()
	}
	pkeysstr := strings.Join(strkeys, ",")
	if len(st.Ukeys) == 0 {
		query = fmt.Sprintf("%v ON CONFLICT (%v) DO NOTHING;", query, pkeysstr)

	} else {
		query = fmt.Sprintf("%v ON CONFLICT (%v) DO UPDATE SET %v;", query, pkeysstr, strings.Join(st.Ukeys, ", "))
	}

	fmt.Printf("Write INSERT to Postgres %v\n", query)
	_, err := s.Exec(query, st.Vals...)
	return err

}

func deleteMsg(m message.Msg, s *sql.DB) error {
	log.With("table", m.Namespace()).With("values", m.Data()).Debugln("DELETE")
	var (
		ckeys []string
		vals  []interface{}
	)
	pkeys, err := primaryKeys(m.Namespace(), s)
	if err != nil {
		return err
	}
	i := 1
	for key, value := range m.Data() {
		if pkeys[key] { // key is primary key
			ckeys = append(ckeys, fmt.Sprintf("%v = $%v", key, i))
		}
		switch value.(type) {
		case map[string]interface{}, mejson.M, []map[string]interface{}, mejson.S:
			value, _ = json.Marshal(value)
		case []interface{}:
			value, _ = json.Marshal(value)
			value = string(value.([]byte))
			value = fmt.Sprintf("{%v}", value.(string)[1:len(value.(string))-1])
		}
		vals = append(vals, value)
		i = i + 1
	}

	if len(pkeys) != len(ckeys) {
		return fmt.Errorf("All primary keys were not accounted for. Provided: %v; Required; %v", ckeys, pkeys)
	}

	query := fmt.Sprintf("DELETE FROM %v WHERE %v;", m.Namespace(), strings.Join(ckeys, " AND "))
	_, err = s.Exec(query, vals...)
	return err
}

func updateMsg(m message.Msg, s *sql.DB) error {
	log.With("table", m.Namespace()).Debugln("UPDATE")
	var (
		ckeys []string
		ukeys []string
		vals  []interface{}
	)

	pkeys, err := primaryKeys(m.Namespace(), s)
	if err != nil {
		return err
	}

	i := 1
	for key, value := range m.Data() {
		if pkeys[key] { // key is primary key
			ckeys = append(ckeys, fmt.Sprintf("%v=$%v", key, i))
		} else {
			ukeys = append(ukeys, fmt.Sprintf("%v=$%v", key, i))
		}

		switch value.(type) {
		case map[string]interface{}, mejson.M, []map[string]interface{}, mejson.S:
			value, _ = json.Marshal(value)
		case []interface{}:
			value, _ = json.Marshal(value)
			value = string(value.([]byte))
			value = fmt.Sprintf("{%v}", value.(string)[1:len(value.(string))-1])
		}
		vals = append(vals, value)
		i = i + 1
	}

	if len(pkeys) != len(ckeys) {
		return fmt.Errorf("All primary keys were not accounted for. Provided: %v; Required; %v", ckeys, pkeys)
	}

	query := fmt.Sprintf("UPDATE %v SET %v WHERE %v;", m.Namespace(), strings.Join(ukeys, ", "), strings.Join(ckeys, " AND "))
	_, err = s.Exec(query, vals...)
	return err
}

func primaryKeys(namespace string, db *sql.DB) (primaryKeys map[string]bool, err error) {
	primaryKeys = map[string]bool{}
	namespaceArray := strings.SplitN(namespace, ".", 2)
	log.Debugf("namespace is: %v", namespace)
	log.Debugf("len namespacearray is: %v", len(namespaceArray))
	var (
		tableSchema string
		tableName   string
		columnName  string
	)

	if len(namespaceArray) > 1 {
		if namespaceArray[1] == "" {
			tableSchema = "public"
			tableName = namespaceArray[0]
		} else {
			tableSchema = namespaceArray[0]
			tableName = namespaceArray[1]
		}
	} else {
		tableSchema = "public"
		tableName = namespaceArray[0]
	}
	log.Debugf("tableschema is: %v", tableSchema)
	log.Debugf("tablename is: %v", tableName)

	tablesResult, err := db.Query(fmt.Sprintf(`
		SELECT
			column_name
		FROM information_schema.table_constraints constraints
			INNER JOIN information_schema.constraint_column_usage column_map
				ON column_map.constraint_name = constraints.constraint_name
		WHERE constraints.constraint_type = 'PRIMARY KEY'
			AND constraints.table_schema = '%v'
			AND constraints.table_name = '%v'
	`, tableSchema, tableName))
	if err != nil {
		return primaryKeys, err
	}

	for tablesResult.Next() {
		err = tablesResult.Scan(&columnName)
		if err != nil {
			return primaryKeys, err
		}
		primaryKeys[columnName] = true
	}

	return primaryKeys, err
}

// GenerateUpsertStatement used to generate set
func GenerateUpsertStatement(m message.Msg, s *sql.DB) (UpsertValues, error) {
	fmt.Printf("Generating UPSERT statement for Postgres %v\n", m.Namespace())
	var (
		inames []string
		ikeys  []string
		ckeys  []string
		ukeys  []string
		vals   []interface{}
	)

	log.Debugf("Prepare primary keys for %v", m.Namespace)
	pkeys, err := primaryKeys(m.Namespace(), s)

	log.Debugf("got primary keys %#v", pkeys)
	if err != nil {
		log.Errorln("Error generating UPSERT statement for Postgres %v : %v", m.Namespace(), err)
	} else {

		i := 1
		for key, value := range m.Data() {
			inames = append(inames, fmt.Sprintf("%v", key))
			ikeys = append(ikeys, fmt.Sprintf("$%v", i))
			if pkeys[key] { // key is primary key
				ckeys = append(ckeys, fmt.Sprintf("%v=$%v", key, i))
			} else {
				ukeys = append(ukeys, fmt.Sprintf("%v=$%v", key, i))
			}

			switch value.(type) {
			case map[string]interface{}:
				value, _ = json.Marshal(value)
			case []interface{}:
				value, _ = json.Marshal(value)
				value = string(value.([]byte))
				value = fmt.Sprintf("{%v}", value.(string)[1:len(value.(string))-1])
			}
			vals = append(vals, value)
			i = i + 1
		}

	}

	return UpsertValues{Inames: inames, Ikeys: ikeys, Ckeys: ckeys, Ukeys: ukeys, Vals: vals, Pkeys: pkeys}, err
}
