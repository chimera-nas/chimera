// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include "common/rbtree.h"

struct test_node {
    int            key;
    struct rb_node node;
};

static void
verify_inorder(
    struct rb_tree *tree,
    int            *expected,
    int             count)
{
    struct test_node *element;
    int               i = 0;

    rb_tree_first(tree, element);

    while (element && i < count) {
        assert(element->key == expected[i]);
        element = rb_tree_next(tree, element);
        i++;
    }

    assert(i == count);
    assert(element == NULL);
} /* verify_inorder */

int
main(
    int   argc,
    char *argv[])
{
    struct rb_tree    tree;
    struct test_node *nodes;
    struct test_node *found;
    int               test_values[]    = { 5, 3, 7, 1, 9, 6, 8, 2, 4 };
    int               expected_order[] = { 1, 2, 3, 4, 5, 6, 7, 8, 9 };
    int               num_nodes        = sizeof(test_values) / sizeof(test_values[0]);

    rb_tree_init(&tree);

    // Allocate and insert nodes
    nodes = malloc(sizeof(struct test_node) * num_nodes);
    for (int i = 0; i < num_nodes; i++) {
        nodes[i].key = test_values[i];
        rb_tree_insert(&tree, key, &nodes[i]);
    }

    // Verify in-order traversal
    verify_inorder(&tree, expected_order, num_nodes);

    // Test exact queries
    rb_tree_query_exact(&tree, 6, key, found);
    assert(found && found->key == 6);

    rb_tree_query_exact(&tree, 10, key, found);
    assert(found == NULL);

    // Test floor queries
    rb_tree_query_floor(&tree, 5, key, found);
    assert(found && found->key == 5);

    rb_tree_query_floor(&tree, 5.5, key, found);
    assert(found && found->key == 5);

    // Test ceil queries
    rb_tree_query_ceil(&tree, 5, key, found);
    assert(found && found->key == 5);

    rb_tree_query_ceil(&tree, 5.5, key, found);
    assert(found && found->key == 6);

    // Remove some nodes and verify tree remains correct
    rb_tree_remove(&tree, &nodes[1].node);  // Remove 3
    rb_tree_remove(&tree, &nodes[5].node);  // Remove 6

    int expected_after_removal[] = { 1, 2, 4, 5, 7, 8, 9 };
    verify_inorder(&tree, expected_after_removal, 7);

    // Cleanup
    rb_tree_destroy(&tree, NULL, NULL);
    free(nodes);

    printf("All tests passed!\n");
    return 0;
} /* main */
